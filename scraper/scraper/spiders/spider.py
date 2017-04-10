# -*- coding: utf-8 -*-

import datetime
import re

import dateparser
import scrapy

from scraper.items import Agency, Mission, Instrument


class CEOSDBSpider(scrapy.Spider):
    name = "ceosdb_scraper"

    def start_requests(self):
        # Define subclasses
        yield scrapy.Request(url='http://database.eohandbook.com/database/agencytable.aspx', callback=self.parse_agencies)
        yield scrapy.Request(url='http://database.eohandbook.com/database/missiontable.aspx', callback=self.prepare_missions)
        yield scrapy.Request(url='http://database.eohandbook.com/database/instrumenttable.aspx', callback=self.prepare_instruments)

    def parse(self, response):
        TITLE_SELECTOR = 'title ::text'
        title = response.css(TITLE_SELECTOR).extract_first().strip()

        if "AGENCY" in title:
            return self.parse_agencies(response)
        elif "MISSIONS" in title:
            return self.parse_missions(response)
        elif "INSTRUMENTS" in title:
            return self.parse_instruments(response)
        # More can be added if needed

    def parse_agencies(self, response):
        TR_SELECTOR = '//*[@id="dgAgencies"]/tr'
        for row in response.xpath(TR_SELECTOR)[1:]:
            agency = row.xpath('td[1]/b/a/text()').extract_first().strip()
            agency_id = row.xpath('td[1]/b/a/@href').extract_first().strip().split('=', 1)[-1]
            country = row.xpath('td[2]/text()').extract_first().strip()
            website = row.xpath('td[3]/a/@href').extract_first().strip()
            num_missions = row.xpath('td[4]/text()').extract_first().strip()
            num_missions = re.match(r'\d*', num_missions).group(0)
            num_instruments = row.xpath('td[5]/text()').extract_first().strip().replace('-', '')
            yield Agency(id = agency_id, name = agency, country = country, website = website)

    def prepare_missions(self, response):
        sel = scrapy.Selector(response)
        viewstate = sel.xpath("//input[@id='__VIEWSTATE']/@value").extract().pop()
        eventvalidation = sel.xpath("//input[@id='__EVENTVALIDATION']/@value").extract().pop()
        data = {'__EVENTTARGET': '', '__EVENTARGUMENT': '', '__LASTFOCUS': '', '__VIEWSTATE': viewstate,
                '__VIEWSTATEGENERATOR': 'ABD5AB5F', '__VIEWSTATEENCRYPTED': '', '__EVENTVALIDATION': eventvalidation,
                'ddlAgency': 'All', 'ddlMissionStatus': 'All', 'tbMission': '', 'ddlLauchYearFilterType': 'All',
                'tbInstruments': '', 'ddlEOLYearFilterType' : 'All', 'tbApplications': '', 'ddlDisplayResults': 'All',
                'ddlRepeatCycleFilter': 'All', 'btFilter': 'Apply+Filter'}
        yield scrapy.FormRequest('http://database.eohandbook.com/database/missiontable.aspx', formdata = data, callback = self.parse_missions)

    def parse_missions(self, response):
        TR_SELECTOR = '//*[@id="gvMissionTable"]/tr'
        for row in response.xpath(TR_SELECTOR)[1:]:
            url = row.xpath('td[1]/b/a/@href').extract_first().strip()
            yield scrapy.Request(url=response.urljoin(url), callback=self.parse_mission)

    def parse_mission(self, response):
        # Settings for date parsing
        date_parsing_settings = {'RELATIVE_BASE': datetime.datetime(2020, 1, 1)}

        # Basic mission information
        mission_name = response.xpath('//*[@id="lblMissionNameShort"]/text()').extract_first().strip()[2:]
        mission_id = response.url.split('=', 1)[-1]
        mission_fullname = response.xpath('//*[@id="lblMissionNameFull"]/text()').extract_first(default='').strip()
        if not mission_fullname:
            mission_fullname = None
        agency_ids = []
        for agency_link in response.xpath('//*[@id="lblMissionAgencies"]/a/@href').extract():
            agency_id = int(agency_link.strip().split('=', 1)[-1])
            if agency_id not in agency_ids:
                agency_ids.append(agency_id)
        status = response.xpath('//*[@id="lblMissionStatus"]/text()').extract_first().strip()
        launch_date = response.xpath('//*[@id="lblLaunchDate"]/text()').extract_first()
        if launch_date:
            launch_date = dateparser.parse(launch_date.strip(), settings=date_parsing_settings)
        eol_date = response.xpath('//*[@id="lblEOLDate"]/text()').extract_first()
        if eol_date:
            eol_date = dateparser.parse(eol_date.strip(), settings=date_parsing_settings)
        applications = response.xpath('//*[@id="lblMissionObjectivesAndApplications"]/text()').extract_first().strip()

        # Orbit details (if existing)
        orbit_type = response.xpath('//*[@id="lblOrbitType"]/text()').extract_first(default='').strip()
        orbit_period = response.xpath('//*[@id="lblOrbitPeriod"]/text()').extract_first(default='').strip()
        orbit_sense = response.xpath('//*[@id="lblOrbitSense"]/text()').extract_first(default='').strip()
        orbit_inclination = response.xpath('//*[@id="lblOrbitInclination"]/text()').extract_first(default='').strip()
        orbit_altitude = response.xpath('//*[@id="lblOrbitAltitude"]/text()').extract_first(default='').strip()
        orbit_longitude = response.xpath('//*[@id="lblOrbitLongitude"]/text()').extract_first(default='').strip()
        orbit_LST = response.xpath('//*[@id="lblOrbitLST"]/text()').extract_first(default='').strip()
        repeat_cycle = response.xpath('//*[@id="lblRepeatCycle"]/text()').extract_first(default='').strip()

        # TODO: Instruments!!!!
        # TODO: Save measurements of mission (and which instrument does each measurement?)

        # Debug information
        print(mission_name, mission_id, mission_fullname, agency_ids, status, launch_date, eol_date, applications,
              orbit_type, orbit_period, orbit_sense, orbit_inclination, orbit_altitude, orbit_longitude, orbit_LST,
              repeat_cycle)

        # Send mission information to pipelines
        yield Mission(id=mission_id, name=mission_name, full_name=mission_fullname, agencies=agency_ids,
                      status=status, launch_date=launch_date, eol_date=eol_date, applications=applications,
                      orbit_type=orbit_type, orbit_period=orbit_period, orbit_sense=orbit_sense,
                      orbit_inclination=orbit_inclination, orbit_altitude=orbit_altitude,
                      orbit_longitude=orbit_longitude, orbit_LST=orbit_LST, repeat_cycle=repeat_cycle)

    def prepare_instruments(self, response):
        sel = scrapy.Selector(response)
        viewstate = sel.xpath("//input[@id='__VIEWSTATE']/@value").extract().pop()
        eventvalidation = sel.xpath("//input[@id='__EVENTVALIDATION']/@value").extract().pop()
        data = {'__EVENTTARGET': '', '__EVENTARGUMENT': '', '__LASTFOCUS': '', '__VIEWSTATE': viewstate,
                '__VIEWSTATEGENERATOR': 'F2417B6C', '__VIEWSTATEENCRYPTED': '', '__EVENTVALIDATION': eventvalidation,
                'ddlAgency': 'All', 'ddlMissionStatus': 'All', 'tbMission': '', 'ddlInstrumentStatus': 'All',
                'ddlLauchYearFilterType': 'All', 'tbInstrument': '', 'ddlInstrumentType': 'All', 'tbApplications': '',
                'ddlInstrumentTechnology': 'All', 'ddlResolutionFilter': 'All', 'ddlWaveband': 'All',
                'ddlDataAccess': 'All', 'ddlDisplayResults': 'All', 'btFilter': 'Apply+Filter'}
        yield scrapy.FormRequest('http://database.eohandbook.com/database/instrumenttable.aspx', formdata = data, callback = self.parse_instruments)

    def parse_instruments(self, response):
        TR_SELECTOR = '//table[@id="gvInstrumentTable"]/tr'
        date_parsing_settings = {'RELATIVE_BASE': datetime.datetime(2020, 1, 1)}
        for row in response.xpath(TR_SELECTOR)[1:]:
            instrument_name = row.xpath('td[1]/b/a/text()').extract_first().strip()
            instrument_id = row.xpath('td[1]/b/a/@href').extract_first().strip().split('=', 1)[-1]
            instrument_fullname = row.xpath('td[1]/text()').extract_first()
            agency_id = row.xpath('td[2]/a/@href').extract_first()
            if agency_id is not None:
                agency_id = agency_id.strip().split('=', 1)[-1]
            else:
                agency_id = row.xpath('td[2]/text()').extract_first()
            # status = row.css("td:nth-child(3) ::text").extract_first().strip()
            # launch_date = row.css("td:nth-child(4) ::text").extract_first()
            # if launch_date is not None:
            #     launch_date = dateparser.parse(launch_date.strip(), settings=date_parsing_settings)
            # eol_date = row.css("td:nth-child(5) ::text").extract_first()
            # if eol_date is not None:
            #     eol_date = dateparser.parse(eol_date.strip(), settings=date_parsing_settings)
            # applications = row.css("td:nth-child(6) ::text").extract_first().strip()
            # orbit_details = row.css("td:nth-child(8) ::text").extract_first().strip()
            print(instrument_name, instrument_id, instrument_fullname, agency_id)
            # self.g.add((mission, CEOSDB_schema.hasStatus, Literal(status)))
            # if launch_date is not None:
            #     self.g.add((mission, CEOSDB_schema.hasLaunchDate, Literal(launch_date)))
            # if eol_date is not None:
            #     self.g.add((mission, CEOSDB_schema.hasEOLDate, Literal(eol_date)))
            # self.g.add((mission, CEOSDB_schema.hasApplications, Literal(applications)))
            # self.g.add((mission, CEOSDB_schema.hasOrbitDetails, Literal(orbit_details)))
            yield Instrument(id=instrument_id, name=instrument_name, full_name=instrument_fullname,
                             agency_id=agency_id)
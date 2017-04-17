# -*- coding: utf-8 -*-

import datetime
import re

import dateparser
import scrapy

from scraper.items import Agency, Mission, Instrument


class CEOSDBSpider(scrapy.Spider):
    name = "ceosdb_scraper"

    instrument_types = ['Atmospheric chemistry',
                        'Atmospheric temperature and humidity sounders',
                        'Cloud profile and rain radars',
                        'Communications',
                        'Data collection',
                        'Earth radiation budget radiometers',
                        'Gravity instruments',
                        'High resolution optical imagers',
                        'Hyperspectral imagers',
                        'Imaging microwave radars',
                        'Imaging multi-spectral radiometers (passive microwave)',
                        'Imaging multi-spectral radiometers (vis/IR)',
                        'In situ',
                        'Lidars',
                        'Lightning sensors',
                        'Magnetic field',
                        'Multiple direction/polarisation radiometers',
                        'Ocean colour instruments',
                        'Other',
                        'Precision orbit',
                        'Radar altimeters',
                        'Scatterometers',
                        'Space environment',
                        'TBD',
                        ]

    instrument_geometries = ['Conical scanning',
                             'Cross-track scanning',
                             'Earth disk scanning',
                             'Limb-scanning',
                             'Nadir-viewing',
                             'Occultation',
                             'Push-broom scanning',
                             'Side-looking',
                             'Steerable viewing',
                             'Whisk-broom scanning',
                             'TBD'
                             ]

    def start_requests(self):
        # For agencies, do brute force requests as there is not a comprehensive list of them
        for i in range(1, 202):
            yield scrapy.Request(url='http://database.eohandbook.com/database/agencysummary.aspx?agencyID=' + str(i),
                                 callback=self.parse_agency, priority=20)
        yield scrapy.Request(url='http://database.eohandbook.com/database/missiontable.aspx',
                             callback=self.prepare_missions, priority=15)
        yield scrapy.Request(url='http://database.eohandbook.com/database/instrumenttable.aspx',
                             callback=self.prepare_instruments, priority=10)

    def parse(self, response):
        TITLE_SELECTOR = 'title ::text'
        title = response.css(TITLE_SELECTOR).extract_first().strip()

        if 'AGENCY' in title:
            return self.parse_agency(response)
        elif 'MISSIONS' in title:
            return self.parse_missions(response)
        elif 'INSTRUMENTS' in title:
            return self.parse_instruments(response)
        # More can be added if needed

    def parse_agency(self, response):
        agency = response.xpath('//*[@id="lblAgencyNameAbbr"]/text()').extract_first(default='').strip()[2:]
        agency_id = response.url.split('=', 1)[-1]
        country = response.xpath('//*[@id="lblAgencyCountry"]/text()').extract_first(default='').strip()
        website = response.xpath('//*[@id="lblAgencyURL"]/a/@href').extract_first(default='').strip()
        if agency:
            yield Agency(id=agency_id, name=agency, country=country, website=website)

    def prepare_missions(self, response):
        sel = scrapy.Selector(response)
        viewstate = sel.xpath("//input[@id='__VIEWSTATE']/@value").extract().pop()
        eventvalidation = sel.xpath("//input[@id='__EVENTVALIDATION']/@value").extract().pop()
        data = {'__EVENTTARGET': '', '__EVENTARGUMENT': '', '__LASTFOCUS': '', '__VIEWSTATE': viewstate,
                '__VIEWSTATEGENERATOR': 'ABD5AB5F', '__VIEWSTATEENCRYPTED': '', '__EVENTVALIDATION': eventvalidation,
                'ddlAgency': 'All', 'ddlMissionStatus': 'All', 'tbMission': '', 'ddlLauchYearFilterType': 'All',
                'tbInstruments': '', 'ddlEOLYearFilterType': 'All', 'tbApplications': '', 'ddlDisplayResults': 'All',
                'ddlRepeatCycleFilter': 'All', 'btFilter': 'Apply+Filter'}
        yield scrapy.FormRequest('http://database.eohandbook.com/database/missiontable.aspx',
                                 formdata=data, callback=self.parse_missions)

    def parse_missions(self, response):
        TR_SELECTOR = '//*[@id="gvMissionTable"]/tr'
        for row in response.xpath(TR_SELECTOR)[1:]:
            url = row.xpath('td[1]/b/a/@href').extract_first().strip()
            yield scrapy.Request(url=response.urljoin(url), callback=self.parse_mission, priority=14)

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
        applications = response.xpath('//*[@id="lblMissionObjectivesAndApplications"]/text()').extract_first(default='').strip()

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
        print('Mission:', mission_name, mission_id, mission_fullname, agency_ids, status, launch_date, eol_date, applications,
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
        yield scrapy.FormRequest('http://database.eohandbook.com/database/instrumenttable.aspx',
                                 formdata=data, callback=self.parse_instruments)

    def parse_instruments(self, response):
        TR_SELECTOR = '//table[@id="gvInstrumentTable"]/tr'
        for row in response.xpath(TR_SELECTOR)[1:]:
            url = row.xpath('td[1]/b/a/@href').extract_first().strip()
            yield scrapy.Request(url=response.urljoin(url), callback=self.parse_instrument, priority=9)

    def parse_instrument(self, response):
        instrument_name = response.xpath('//*[@id="lblInstrumentNameShort"]/text()').extract_first().strip()[2:]
        instrument_id = response.url.split('=', 1)[-1]
        instrument_fullname = response.xpath('//*[@id="lblInstrumentNameFull"]/text()').extract_first(default='')
        if not instrument_fullname:
            instrument_fullname = None
        status = response.xpath('//*[@id="lblInstrumentStatus"]/text()').extract_first().strip()
        agency_ids = []
        for agency_link in response.xpath('//*[@id="lblInstrumentAgencies"]/a/@href').extract()[:-1]:
            agency_id = int(agency_link.strip().split('=', 1)[-1])
            if agency_id not in agency_ids:
                agency_ids.append(agency_id)
        maturity = response.xpath('//*[@id="lblInstrumentMaturity"]/text()').extract_first(default='').strip()
        types = []
        types_texts = response.xpath('//*[@id="lblInstrumentType"]/text()')
        types_text = ''
        for type_subtext in types_texts.extract():
            types_text += ' ' + type_subtext.strip()
        types_text = types_text.strip()
        for type_template in self.instrument_types:
            if type_template in types_text:
                types.append(type_template)
        geometry_text = response.xpath('//*[@id="lblInstrumentGeometry"]/text()').extract_first(default='').strip()
        geometries = []
        for geometry_template in self.instrument_geometries:
            if geometry_template in geometry_text:
                geometries.append(geometry_template)
        technology = response.xpath('//*[@id="lblInstrumentTechnology"]/text()').extract_first(default='').strip()
        if technology == '':
            technology = None
        # status = row.css("td:nth-child(3) ::text").extract_first().strip()
        # launch_date = row.css("td:nth-child(4) ::text").extract_first()
        # if launch_date is not None:
        #     launch_date = dateparser.parse(launch_date.strip(), settings=date_parsing_settings)
        # eol_date = row.css("td:nth-child(5) ::text").extract_first()
        # if eol_date is not None:
        #     eol_date = dateparser.parse(eol_date.strip(), settings=date_parsing_settings)
        # applications = row.css("td:nth-child(6) ::text").extract_first().strip()
        # orbit_details = row.css("td:nth-child(8) ::text").extract_first().strip()
        print('Instrument:', instrument_name, instrument_id, instrument_fullname, agency_ids, status, maturity, types,
              geometries, technology)
        # self.g.add((mission, CEOSDB_schema.hasStatus, Literal(status)))
        # if launch_date is not None:
        #     self.g.add((mission, CEOSDB_schema.hasLaunchDate, Literal(launch_date)))
        # if eol_date is not None:
        #     self.g.add((mission, CEOSDB_schema.hasEOLDate, Literal(eol_date)))
        # self.g.add((mission, CEOSDB_schema.hasApplications, Literal(applications)))
        # self.g.add((mission, CEOSDB_schema.hasOrbitDetails, Literal(orbit_details)))
        yield Instrument(id=instrument_id, name=instrument_name, full_name=instrument_fullname,
                         agencies=agency_ids, status=status, maturity=maturity, types=types, geometries=geometries,
                         technology=technology)
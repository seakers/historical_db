# -*- coding: utf-8 -*-

import datetime
import re

import dateparser
import scrapy

from scraper.items import BroadMeasurementCategory, MeasurementCategory, Agency, Mission, Instrument


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

    mission_ids = []

    def start_requests(self):
        yield scrapy.Request(url='http://database.eohandbook.com/measurements/overview.aspx',
                             callback=self.prepare_broad_categories, priority=25)
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

    def prepare_broad_categories(self, response):
        broad_cat_links = \
            response.xpath('//*[@id="pnlNominal"]/tr[2]/td/table/tr[2]/td/table/tr/td[1]/table/tr/td/a[not(img)]/@href')\
                .extract()
        for link in broad_cat_links:
            url = link.strip()
            yield scrapy.Request(url=response.urljoin(url), callback=self.parse_broad_category, priority=24)

    def parse_broad_category(self, response):
        bc_id = response.url.split('=', 1)[-1]
        name = response.xpath('//*[@id="pnlNominal"]/tr[2]/td/table/tr/td/table/tr[1]/td[1]/b/text()')\
            .extract_first().strip()[2:]
        description = response.xpath('//*[@id="pnlNominal"]/tr[2]/td/table/tr/td/table/tr[1]/td[2]/text()')\
            .extract_first().strip()

        yield BroadMeasurementCategory(id=bc_id, name=name, description=description)

        categories = response.xpath('//*[@id="pnlNominal"]/tr[2]/td/table/tr/td/table/tr[2]/td/table/tr/td[1]/a/@href') \
            .extract()
        for category_link in categories:
            yield scrapy.Request(url=response.urljoin(category_link), callback=self.parse_category, priority=23)

    def parse_category(self, response):
        c_id = response.url.split('=', 1)[-1]
        name = response.xpath('//*[@id="pnlNominal"]/tr[2]/td/table/tr/td/table/tr[1]/td[1]/b/text()') \
                       .extract()[2].strip()
        description = response.xpath('//*[@id="pnlNominal"]/tr[2]/td/table/tr/td/table/tr[1]/td[2]/text()') \
                              .extract_first().strip()
        broad_category_id = response.xpath('//*[@id="pnlNominal"]/tr[2]/td/table/tr/td/table/tr[1]/td[1]/b/a[2]/@href') \
                                    .extract_first().strip().split('=')[-1]

        yield MeasurementCategory(id=c_id, name=name, description=description, broad_category_id=broad_category_id)


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

        # TODO: Save measurements of mission (and which instrument does each measurement?)

        # Debug information
        print('Mission:', mission_name, mission_id, mission_fullname, agency_ids, status, launch_date, eol_date, applications,
              orbit_type, orbit_period, orbit_sense, orbit_inclination, orbit_altitude, orbit_longitude, orbit_LST,
              repeat_cycle)

        # Save information for later
        self.mission_ids.append(int(mission_id))

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
        # Basic instrument information
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
        sampling = response.xpath('//*[@id="lblInstrumentSampling"]/text()').extract_first(default='').strip()
        if sampling == '':
            sampling = None
        data_access = response.xpath('//*[@id="lblDataAccess"]/text()').extract_first(default='').strip()
        if data_access == '':
            data_access = None
        data_format = response.xpath('//*[@id="lblDataFormat"]/text()').extract_first(default='').strip()
        if data_format == '':
            data_format = None

        # Measurements summary
        measurements_and_applications = \
            response.xpath('//*[@id="lblInstrumentMeasurementsApplications"]/text()').extract_first(default='').strip()

        # Missions
        missions = []
        for mission_link in response.xpath('//*[@id="pnlNominal"]/tr[1]/td/table/tr[18]/td[2]/table/tr/td/a/@href').extract():
            mission_id = int(mission_link.strip().split('=', 1)[-1])
            if mission_id not in missions and mission_id in self.mission_ids:
                missions.append(mission_id)

        # TODO: Measurements!!
        # TODO: Summaries!!
        # TODO: Frequencies!!

        # Debug information
        print('Instrument:', instrument_name, instrument_id, instrument_fullname, agency_ids, status, maturity, types,
              geometries, technology, sampling, data_access, data_format, measurements_and_applications, missions)

        # Send Instrument information to pipelines
        yield Instrument(id=instrument_id, name=instrument_name, full_name=instrument_fullname,
                         agencies=agency_ids, status=status, maturity=maturity, types=types, geometries=geometries,
                         technology=technology, sampling=sampling, data_access=data_access, data_format=data_format,
                         measurements_and_applications=measurements_and_applications, missions=missions)

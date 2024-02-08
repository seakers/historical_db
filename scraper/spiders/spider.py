# -*- coding: utf-8 -*-

import datetime
import re

import dateparser
import scrapy

from scraper.items import BroadMeasurementCategory, MeasurementCategory, Measurement, Agency, Mission, Instrument


# Patch for gcos links in XML doc
def remove_gcos_links(links):
    new_links = []
    remove_link = 'gcos.wmo.int'
    for link in links:
        if remove_link not in link:
            new_links.append(link)
    print("---> OLD LINKS", links)
    print("---> NEW LINKS", new_links)
    return new_links



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

    wavebands = [('UV', '~0.01 µm - ~0.40 µm'),
                 ('VIS', '~0.40 µm - ~0.75 µm'),
                 ('NIR', '~0.75 µm - ~1.3 µm'),
                 ('SWIR', '~1.3 µm - ~3.0 µm'),
                 ('MWIR', '~3.0 µm - ~6.0 µm'),
                 ('TIR', '~6.0 µm - ~15.0 µm'),
                 ('FIR', '~15.0 µm - ~0.1 cm'),
                 ('MW', '~1.0 cm - ~100 cm'),
                 ('W-Band', '94 GHz'),
                 ('Ka-Band', '40 - 26.5 GHz'),
                 ('K-Band', '26.5 - 18 GHz'),
                 ('Ku-Band', '18 - 12.5 GHz'),
                 ('X-Band', '12.5 - 8 GHz'),
                 ('C-Band', '8 - 4 GHz'),
                 ('S-Band', '4 - 2 GHz'),
                 ('L-Band', '2 - 1 GHz'),
                 ('P-Band', '0.999 - 0.2998 GHz'),
                 ('TBD', ''),
                 ('N/A', '')
 ]

    mission_ids = []
    measurment_ids = []

    def start_requests(self):
        yield scrapy.Request(url='http://database.eohandbook.com/measurements/overview.aspx',
                             callback=self.prepare_broad_categories, priority=25)
        # For agencies, do brute force requests as there is not a comprehensive list of them
        for i in range(1, 230):
            yield scrapy.Request(url='http://database.eohandbook.com/database/agencysummary.aspx?agencyID=' + str(i),
                                 callback=self.parse_agency, priority=20)
        
        # TODO: the update to the CEOS database website seems to have broken the ddlDisplayResults being set to "All", so the commented-out code below
        #       only works for the first 10 missions/instruments. I think this is solvable but I tried for several hours with no luck.
        # yield scrapy.Request(url='http://database.eohandbook.com/database/missiontable.aspx',
        #                      callback=self.prepare_missions, priority=15)
        # yield scrapy.Request(url='http://database.eohandbook.com/database/missiontable.aspx',
        #                      callback=self.prepare_instruments, priority=15)
        for i in range(0,1450):
            yield scrapy.Request(url='http://database.eohandbook.com/database/missionsummary.aspx?missionID='+str(i),
                                callback=self.parse_mission, priority=10)
        for i in range(0,2108):
            yield scrapy.Request(url='http://database.eohandbook.com/database/instrumentsummary.aspx?instrumentID='+str(i),
                                callback=self.parse_instrument, priority=10)

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
            response.xpath('//*[@id="MainContent_pnlNominal"]/tr[2]/td/table/tr[2]/td/table/tr/td[1]/table/tr/td/a[not(img)]/@href')\
                .extract()
        for link in broad_cat_links:
            url = link.strip()
            yield scrapy.Request(url=response.urljoin(url), callback=self.parse_broad_category, priority=24)

    def parse_broad_category(self, response):
        bc_id = int(response.url.split('=', 1)[-1])
        name = response.xpath('//*[@id="MainContent_pnlNominal"]/tr[2]/td/table/tr/td/table/tr[1]/td[1]/b/text()')\
            .extract_first().strip()[2:]
        description = response.xpath('//*[@id="MainContent_pnlNominal"]/tr[2]/td/table/tr/td/table/tr[1]/td[2]/text()')\
            .extract_first().strip()

        print('Broad category:', bc_id, name, description)
        yield BroadMeasurementCategory(id=bc_id, name=name, description=description)

        categories = response.xpath('//*[@id="MainContent_pnlNominal"]/tr[2]/td/table/tr/td/table/tr[2]/td/table/tr/td[1]/a/@href') \
            .extract()
        for category_link in categories:
            yield scrapy.Request(url=response.urljoin(category_link), callback=self.parse_category, priority=23)

    def parse_category(self, response):
        c_id = int(response.url.split('=', 1)[-1])
        name = response.xpath('//*[@id="MainContent_pnlNominal"]/tr[2]/td/table/tr/td/table/tr[1]/td[1]/b/text()') \
                       .extract()[2].strip()
        description = response.xpath('//*[@id="MainContent_pnlNominal"]/tr[2]/td/table/tr/td/table/tr[1]/td[2]/text()') \
                              .extract_first().strip()
        broad_category_id = response.xpath('//*[@id="MainContent_pnlNominal"]/tr[2]/td/table/tr/td/table/tr[1]/td[1]/b/a[2]/@href') \
                                    .extract_first().strip().split('=')[-1]

        print('Category:', c_id, name, description, broad_category_id)
        yield MeasurementCategory(id=c_id, name=name, description=description,
                                  broad_measurement_category_id=broad_category_id)

        measurement_rows = response.xpath('//*[@id="MainContent_pnlNominal"]/tr[2]/td/table/tr[1]/td/table/tr[2]/td/table/tr')
        for measurement_row in measurement_rows[1:]:
            m_id = int(measurement_row.xpath('td[1]/a/@href').extract_first().strip().split('=', 1)[-1])
            m_name = measurement_row.xpath('td[1]/a/b/text()').extract_first().strip()
            m_description = measurement_row.xpath('td[2]/text()').extract_first().strip()
            print('Measurement:', m_id, m_name, m_description, c_id)
            self.measurment_ids.append(m_id)
            yield Measurement(id=m_id, name=m_name, description=m_description, measurement_category_id=c_id)


    def parse_agency(self, response):
        agency = response.xpath('//*[@id="MainContent_lblAgencyNameAbbr"]/text()').extract_first(default='').strip()[2:]
        agency_id = int(response.url.split('=', 1)[-1])
        country = response.xpath('//*[@id="MainContent_lblAgencyCountry"]/text()').extract_first(default='').strip()
        website = response.xpath('//*[@id="MainContent_lblAgencyURL"]/a/@href').extract_first(default='').strip()
        if agency:
            print('Agency:', agency, agency_id, country, website)
            yield Agency(id=agency_id, name=agency, country=country, website=website)

    def prepare_missions(self, response):
        sel = scrapy.Selector(response)
        viewstate = sel.xpath("//input[@id='__VIEWSTATE']/@value").extract().pop()
        eventvalidation = sel.xpath("//input[@id='__EVENTVALIDATION']/@value").extract().pop()
        data = {'__EVENTTARGET': '', '__EVENTARGUMENT': '', '__LASTFOCUS': '', '__VIEWSTATE': viewstate,
                '__VIEWSTATEGENERATOR': 'ABD5AB5F', '__VIEWSTATEENCRYPTED': '', '__EVENTVALIDATION': eventvalidation,
                'MainContent_ddlAgency': 'All', 'MainContent_ddlMissionStatus': 'All', 'MainContent_tbMission': '', 'MainContent_ddlLauchYearFilterType': 'All',
                'MainContent_tbInstruments': '', 'MainContent_ddlEOLYearFilterType': 'All', 'MainContent_tbApplications': '', 'MainContent_ddlDisplayResults': 'All',
                'MainContent_ddlRepeatCycleFilter': 'All', 'MainContent_btFilter': 'Apply+Filter'}
        yield scrapy.FormRequest.from_response(response,
                                 formdata=data, callback=self.parse_missions)

    def parse_missions(self, response):
        TR_SELECTOR = '//*[@id="MainContent_gvMissionTable"]/tr'
        for row in response.xpath(TR_SELECTOR)[1:]:
            url = row.xpath('td[1]/b/a/@href').extract_first().strip()
            yield scrapy.Request(url=response.urljoin(url), callback=self.parse_mission, priority=14)

    def parse_mission(self, response):
        if "pnlError" in response.text:
            return
        # Settings for date parsing
        date_parsing_settings = {'RELATIVE_BASE': datetime.datetime(2020, 1, 1)}

        # Basic mission information
        mission_name = response.xpath('//*[@id="MainContent_lblMissionNameShort"]/text()').extract_first().strip()[2:]
        mission_id = int(response.url.split('=', 1)[-1])
        mission_fullname = response.xpath('//*[@id="MainContent_lblMissionNameFull"]/text()').extract_first(default='').strip()
        if not mission_fullname:
            mission_fullname = None
        agency_ids = []
        for agency_link in response.xpath('//*[@id="MainContent_lblMissionAgencies"]/a/@href').extract():
            agency_id = int(agency_link.strip().split('=', 1)[-1])
            if agency_id not in agency_ids:
                agency_ids.append(agency_id)
        status = response.xpath('//*[@id="MainContent_lblMissionStatus"]/text()').extract_first().strip()
        launch_date = response.xpath('//*[@id="MainContent_lblLaunchDate"]/text()').extract_first()
        if launch_date:
            launch_date = dateparser.parse(launch_date.strip(), settings=date_parsing_settings)
        eol_date = response.xpath('//*[@id="MainContent_lblEOLDate"]/text()').extract_first()
        if eol_date:
            eol_date = dateparser.parse(eol_date.strip(), settings=date_parsing_settings)
        norad_id = response.xpath('//*[@id="MainContent_lblNoradNumberLink"]/a/text()').extract_first()
        if norad_id is not None:
            norad_id = int(norad_id)

        applications = response.xpath('//*[@id="MainContent_lblMissionObjectivesAndApplications"]/text()').extract_first(default='').strip()

        # Orbit details (if existing)
        orbit_type = response.xpath('//*[@id="MainContent_lblOrbitType"]/text()').extract_first(default='').strip()
        orbit_period = response.xpath('//*[@id="MainContent_lblOrbitPeriod"]/text()').extract_first(default='').strip()
        orbit_sense = response.xpath('//*[@id="MainContent_lblOrbitSense"]/text()').extract_first(default='').strip()

        orbit_inclination = response.xpath('//*[@id="MainContent_lblOrbitInclination"]/text()').extract_first(default='').strip()
        if orbit_inclination == '':
            orbit_inclination = None
            orbit_inclination_num = None
            orbit_inclination_class = None
        else:
            orbit_inclination_num = float(orbit_inclination[:-4])
            if orbit_inclination_num == 0.0:
                orbit_inclination_class = 'Equatorial'
            elif orbit_inclination_num < 30.0:
                orbit_inclination_class = 'Near Equatorial'
            elif orbit_inclination_num < 60.0:
                orbit_inclination_class = 'Mid Latitude'
            elif orbit_inclination_num == 90.0:
                orbit_inclination_class = 'Polar'
            else:
                orbit_inclination_class = 'Near Polar'

        orbit_altitude = response.xpath('//*[@id="MainContent_lblOrbitAltitude"]/text()').extract_first(default='').strip()
        if orbit_altitude == '':
            orbit_altitude = None
            orbit_altitude_num = None
            orbit_altitude_class = None
        else:
            orbit_altitude_num = int(orbit_altitude[:-3])
            if orbit_altitude_num < 400:
                orbit_altitude_class = 'VL'
            elif orbit_altitude_num < 550:
                orbit_altitude_class = 'L'
            elif orbit_altitude_num < 700:
                orbit_altitude_class = 'M'
            elif orbit_altitude_num < 850:
                orbit_altitude_class = 'H'
            else:
                orbit_altitude_class = 'VH'

        orbit_longitude = response.xpath('//*[@id="MainContent_lblOrbitLongitude"]/text()').extract_first(default='').strip()
        orbit_LST = response.xpath('//*[@id="MainContent_lblOrbitLST"]/text()').extract_first(default='').strip()
        if orbit_LST == '':
            orbit_LST = None
            orbit_LST_time = None
            orbit_LST_class = None
        else:
            orbit_LST_time = dateparser.parse(orbit_LST)
            if orbit_LST_time is not None:
                orbit_LST_time = orbit_LST_time.time()
                five_am = datetime.time(5)
                seven_am = datetime.time(7)
                five_pm = datetime.time(17)
                seven_pm = datetime.time(19)
                noon_am = datetime.time(11, 15)
                noon_pm = datetime.time(12, 45)
                time_for_class = orbit_LST_time
                if time_for_class < five_am:
                    time_for_class = (datetime.datetime.combine(datetime.date.today(), time_for_class) +
                                      datetime.timedelta(hours=12)).time()
                elif time_for_class > seven_pm:
                    time_for_class = (datetime.datetime.combine(datetime.date.today(), time_for_class) -
                                      datetime.timedelta(hours=12)).time()

                orbit_LST_class = None
                if time_for_class > five_am and time_for_class < seven_am:
                    orbit_LST_class = 'DD'
                elif time_for_class > five_pm and time_for_class < seven_pm:
                    orbit_LST_class = 'DD'
                elif time_for_class > noon_am and time_for_class < noon_pm:
                    orbit_LST_class = 'Noon'
                elif time_for_class > seven_am and time_for_class < noon_am:
                    orbit_LST_class = 'AM'
                elif time_for_class > noon_pm and time_for_class < five_pm:
                    orbit_LST_class = 'PM'
            else:
                orbit_LST_time = None
                orbit_LST_class = None


        repeat_cycle = response.xpath('//*[@id="MainContent_lblRepeatCycle"]/text()').extract_first(default='').strip()
        if repeat_cycle == '':
            repeat_cycle = None
            repeat_cycle_num = None
            repeat_cycle_class = None
        else:
            repeat_cycle_num = float(repeat_cycle[:-5])
            if repeat_cycle_num <= 7:
                repeat_cycle_class = 'Short'
            else:
                repeat_cycle_class = 'Long'

        # Debug information
        print('Mission:', mission_name, mission_id, mission_fullname, agency_ids, status, launch_date, eol_date,
              norad_id, applications, orbit_type, orbit_period, orbit_sense, orbit_inclination, orbit_inclination_num,
              orbit_inclination_class, orbit_altitude, orbit_altitude_num, orbit_altitude_class, orbit_longitude,
              orbit_LST, orbit_LST_time, orbit_LST_class, repeat_cycle, repeat_cycle_num, repeat_cycle_class)

        # Save information for later
        self.mission_ids.append(mission_id)

        # Send mission information to pipelines
        yield Mission(id=mission_id, name=mission_name, full_name=mission_fullname, agencies=agency_ids,
                      status=status, launch_date=launch_date, eol_date=eol_date, norad_id=norad_id,
                      applications=applications, orbit_type=orbit_type, orbit_period=orbit_period,
                      orbit_sense=orbit_sense, orbit_inclination=orbit_inclination,
                      orbit_inclination_num=orbit_inclination_num, orbit_inclination_class=orbit_inclination_class,
                      orbit_altitude=orbit_altitude, orbit_altitude_num=orbit_altitude_num,
                      orbit_altitude_class=orbit_altitude_class, orbit_longitude=orbit_longitude, orbit_LST=orbit_LST,
                      orbit_LST_time=orbit_LST_time, orbit_LST_class=orbit_LST_class, repeat_cycle=repeat_cycle,
                      repeat_cycle_num=repeat_cycle_num, repeat_cycle_class=repeat_cycle_class)

    def prepare_instruments(self, response):
        sel = scrapy.Selector(response)
        viewstate = sel.xpath("//input[@id='__VIEWSTATE']/@value").extract().pop()
        eventvalidation = sel.xpath("//input[@id='__EVENTVALIDATION']/@value").extract().pop()
        data = {'__EVENTTARGET': '', '__EVENTARGUMENT': '', '__LASTFOCUS': '', '__VIEWSTATE': viewstate,
                '__VIEWSTATEGENERATOR': 'F2417B6C', '__VIEWSTATEENCRYPTED': '', '__EVENTVALIDATION': eventvalidation,
                'MainContent_ddlAgency': 'All', 'MainContent_ddlMissionStatus': 'All', 'MainContent_tbMission': '', 'MainContent_ddlInstrumentStatus': 'All',
                'MainContent_tbInstrument': '', 'MainContent_ddlInstrumentType': 'All', 'MainContent_tbApplications': '',
                'MainContent_ddlInstrumentTechnology': 'All', 'MainContent_ddlResolutionFilter': 'All', 'MainContent_ddlWaveband': 'All',
                'MainContent_ddlDataAccess': 'All', 'MainContent_ddlDisplayResults': "3", 'MainContent_btFilter': 'Apply+Filter'}
        yield scrapy.FormRequest.from_response(response,
                                 formdata=data, dont_click=True, callback=self.parse_instruments)

    def parse_instruments(self, response):
        TR_SELECTOR = '//table[@id="MainContent_gvInstrumentTable"]/tr'
        for row in response.xpath(TR_SELECTOR)[1:]:
            url = row.xpath('td[1]/b/a/@href').extract_first().strip()
            yield scrapy.Request(url=response.urljoin(url), callback=self.parse_instrument, priority=9)

    def parse_instrument(self, response):
        # Basic instrument information
        if "pnlError" in response.text:
            return
        instrument_name = response.xpath('//*[@id="MainContent_lblInstrumentNameShort"]/text()').extract_first().strip()[2:]
        instrument_id = int(response.url.split('=', 1)[-1])
        instrument_fullname = response.xpath('//*[@id="MainContent_lblInstrumentNameFull"]/text()').extract_first(default='')
        print('---> INSTRUMENT NAME ', instrument_name)
        print('---> URL ', response.url)
        if not instrument_fullname:
            instrument_fullname = None
        status = response.xpath('//*[@id="MainContent_lblInstrumentStatus"]/text()').extract_first().strip()
        agency_ids = []
        for agency_link in response.xpath('//*[@id="MainContent_lblInstrumentAgencies"]/a/@href').extract()[:-1]:
            agency_id = int(agency_link.strip().split('=', 1)[-1])
            if agency_id not in agency_ids:
                agency_ids.append(agency_id)
        maturity = response.xpath('//*[@id="MainContent_lblInstrumentMaturity"]/text()').extract_first(default='').strip()
        types = []
        types_texts = response.xpath('//*[@id="MainContent_lblInstrumentType"]/text()')
        types_text = ''
        for type_subtext in types_texts.extract():
            types_text += ' ' + type_subtext.strip()
        types_text = types_text.strip()
        for type_template in self.instrument_types:
            if type_template in types_text:
                types.append(type_template)
        geometry_text = response.xpath('//*[@id="MainContent_lblInstrumentGeometry"]/text()').extract_first(default='').strip()
        geometries = []
        for geometry_template in self.instrument_geometries:
            if geometry_template in geometry_text:
                geometries.append(geometry_template)
        technology = response.xpath('//*[@id="MainContent_lblInstrumentTechnology"]/text()').extract_first(default='').strip()
        if technology == '':
            technology = None
        sampling = response.xpath('//*[@id="MainContent_lblInstrumentSampling"]/text()').extract_first(default='').strip()
        if sampling == '':
            sampling = None
        data_access = response.xpath('//*[@id="MainContent_lblDataAccess"]/text()').extract_first(default='').strip()
        if data_access == '':
            data_access = None
        data_format = response.xpath('//*[@id="MainContent_lblDataFormat"]/text()').extract_first(default='').strip()
        if data_format == '':
            data_format = None

        # Measurements summary
        measurements_and_applications = \
            response.xpath('//*[@id="MainContent_lblInstrumentMeasurementsApplications"]/text()').extract_first(default='').strip()

        # Missions
        missions = []
        for mission_link in response.xpath('//*[@id="MainContent_pnlNominal"]/tr[1]/td/table/tr[18]/td[2]/table/tr/td/a/@href').extract():
            mission_id = int(mission_link.strip().split('=', 1)[-1])
            if mission_id not in missions and mission_id in self.mission_ids:
                missions.append(mission_id)

        # Measurements
        measurements = []
        accuracies = []
        measurement_links = response.xpath('//*[@id="MainContent_pnlNominal"]/tr[1]/td/table/tr[16]/td[2]/table/tr/td[2]/a/@href')
        extracted_measurement_links = remove_gcos_links(measurement_links.extract())
        # for link in measurement_links.extract():
        for link in extracted_measurement_links:
            m_id = int(link.strip().split('=', 1)[-1])
            if m_id not in self.measurment_ids:
                m_name = response.xpath('//*[@id="MainContent_pnlNominal"]/tr[1]/td/table/tr[16]/td[2]/table/tr/td[2]/a/text()')\
                    .extract_first().strip()
                self.measurment_ids.append(m_id)
                yield Measurement(id=m_id, name=m_name, description='', measurement_category_id=1000)
            measurements.append(m_id)
            accuracies.append('50km h-code')

        # accuracy_infos = response.xpath('//*[@id="MainContent_pnlNominal"]/tr[1]/td/table/tr[16]/td[2]/table/tr[position()>1]/td[3]/text()')
        # print("----- ALL ACCURACY LINKS",  accuracy_infos.extract())
        # for accuracy_info in accuracy_infos.extract():
        #     accuracy = accuracy_info.strip()
        #     accuracies.append(accuracy)

        # Summaries
        resolution_summary = response.xpath('//*[@id="MainContent_lblInstrumentResolutionSummary"]/text()').extract_first(default='').strip()
        if resolution_summary == '':
            resolution_summary = None
        best_resolution = response.xpath('//*[@id="MainContent_lblInstrumentResolutionSummary"]/i/text()').extract_first(default='').strip()
        if best_resolution == '':
            best_resolution = None
        else:
            best_resolution = best_resolution[1:-1].split(':', 1)[-1].strip()
        swath_summary = response.xpath('//*[@id="MainContent_lblInstrumentSwathSummary"]/text()').extract_first(default='').strip()
        if swath_summary == '':
            swath_summary = None
        max_swath = response.xpath('//*[@id="MainContent_lblInstrumentSwathSummary"]/i/text()').extract_first(default='').strip()
        if max_swath == '':
            max_swath = None
        else:
            max_swath = max_swath[1:-1].split(':', 1)[-1].strip()
        accuracy_summary = response.xpath('//*[@id="MainContent_lblInstrumentAccuracySummary"]/text()').extract_first(default='').strip()
        if accuracy_summary == '':
            accuracy_summary = None
        waveband_summary = response.xpath('//*[@id="MainContent_lblInstrumentWavebandSummary"]/text()').extract_first(default='').strip()
        if waveband_summary == '':
            waveband_summary = None

        # Frequencies
        wavebands = []
        waveband_list = response.xpath('//*[@id="MainContent_pnlNominal"]/tr[1]/td/table/tr[14]/td[2]/i/table/tr/td/text()').extract()
        for waveband in waveband_list:
            w_name = waveband.split('(', 1)[0].strip()
            if w_name != '':
                wavebands.append(w_name)

        # Debug information
        print('Instrument:', instrument_name, instrument_id, instrument_fullname, agency_ids, status, maturity, types,
              geometries, technology, sampling, data_access, data_format, measurements_and_applications, missions,
              measurements, accuracies, resolution_summary, best_resolution, swath_summary, max_swath, accuracy_summary,
              waveband_summary, wavebands)

        # Send Instrument information to pipelines
        yield Instrument(id=instrument_id, name=instrument_name, full_name=instrument_fullname,
                         agencies=agency_ids, status=status, maturity=maturity, types=types, geometries=geometries,
                         technology=technology, sampling=sampling, data_access=data_access, data_format=data_format,
                         measurements_and_applications=measurements_and_applications, missions=missions,
                         measurements=measurements, accuracies=accuracies, resolution_summary=resolution_summary,
                         best_resolution=best_resolution, swath_summary=swath_summary, max_swath=max_swath,
                         accuracy_summary=accuracy_summary, waveband_summary=waveband_summary, wavebands=wavebands)

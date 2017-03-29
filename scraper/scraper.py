import re
import scrapy
import datetime
import dateparser
import CEOSDB_schema
from rdflib import Graph, Literal, BNode, Namespace, RDF, RDFS, URIRef
from rdflib.namespace import FOAF, OWL

class CEOSDBSpider(scrapy.Spider):
    name = "ceosdb_spider"
    g = Graph()

    def start_requests(self):
        # Define subclasses
        self.define_subclasses()
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
        TR_SELECTOR = "#dgAgencies > tr"
        for row in response.css(TR_SELECTOR)[1:]:
            agency = row.css("td:nth-child(1) b a ::text").extract_first().strip()
            agency_id = row.css("td:nth-child(1) b a ::attr(href)").extract_first().strip().split('=', 1)[-1]
            country = row.css("td:nth-child(2) ::text").extract_first().strip()
            website = row.css("td:nth-child(3) a ::attr(href)").extract_first().strip()
            num_missions = row.css("td:nth-child(4) ::text").extract_first().strip()
            num_missions = re.match(r"\d*", num_missions).group(0)
            num_instruments = row.css("td:nth-child(5) ::text").extract_first().strip().replace("-", "")
            sa = URIRef("http://ceosdb/agency#" + agency_id)
            self.g.add((sa, RDFS.label, Literal(agency)))
            self.g.add((sa, RDF.type, CEOSDB_schema.agencyClass))
            self.g.add((sa, CEOSDB_schema.isFromCountry, Literal(country)))
            self.g.add((sa, FOAF.homepage, URIRef(website)))

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
        TR_SELECTOR = "#gvMissionTable > tr"
        date_parsing_settings = {'RELATIVE_BASE': datetime.datetime(2020, 1, 1)}
        for row in response.css(TR_SELECTOR)[1:-1]:
            mission_name = row.css("td:nth-child(1) b a ::text").extract_first().strip()
            mission_id = row.css("td:nth-child(1) b a ::attr(href)").extract_first().strip().split('=', 1)[-1]
            mission_fullname = row.css("td:nth-child(1) ::text").extract()
            if len(mission_fullname) > 1:
                mission_fullname = mission_fullname[1].strip()
            else:
                mission_fullname = None
            agency_id = row.css("td:nth-child(2) a ::attr(href)").extract_first().strip().split('=', 1)[-1]
            status = row.css("td:nth-child(3) ::text").extract_first().strip()
            launch_date = row.css("td:nth-child(4) ::text").extract_first()
            if launch_date is not None:
                launch_date = dateparser.parse(launch_date.strip(), settings=date_parsing_settings)
            eol_date = row.css("td:nth-child(5) ::text").extract_first()
            if eol_date is not None:
                eol_date = dateparser.parse(eol_date.strip(), settings=date_parsing_settings)
            applications = row.css("td:nth-child(6) ::text").extract_first().strip()
            # TODO: Instruments!!!!
            orbit_details = row.css("td:nth-child(8) ::text").extract_first().strip() # TODO: Save more detailed orbits!
            # TODO: Save measurements of mission (and which instrument does each measurement?)
            print(mission_name, mission_id, mission_fullname, agency_id, status, launch_date, eol_date, applications, orbit_details)
            mission = URIRef("http://ceosdb/mission#" + mission_id)
            self.g.add((mission, RDFS.label, Literal(mission_name)))
            self.g.add((mission, RDF.type, CEOSDB_schema.missionClass))
            if mission_fullname is not None:
                self.g.add((mission, CEOSDB_schema.hasFullName, Literal(mission_fullname)))
            self.g.add((mission, CEOSDB_schema.builtBy, URIRef("http://ceosdb/agency#" + agency_id)))
            self.g.add((mission, CEOSDB_schema.hasStatus, Literal(status)))
            if launch_date is not None:
                self.g.add((mission, CEOSDB_schema.hasLaunchDate, Literal(launch_date)))
            if eol_date is not None:
                self.g.add((mission, CEOSDB_schema.hasEOLDate, Literal(eol_date)))
            self.g.add((mission, CEOSDB_schema.hasApplications, Literal(applications)))
            self.g.add((mission, CEOSDB_schema.hasOrbitDetails, Literal(orbit_details)))

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
        TR_SELECTOR = "#gvInstrumentTable > tr"
        date_parsing_settings = {'RELATIVE_BASE': datetime.datetime(2020, 1, 1)}
        for row in response.css(TR_SELECTOR)[1:-1]:
            instrument_name = row.css("td:nth-child(1) b a ::text").extract_first().strip()
            instrument_id = row.css("td:nth-child(1) b a ::attr(href)").extract_first().strip().split('=', 1)[-1]
            instrument_fullname = row.css("td:nth-child(1) ::text").extract()
            if len(instrument_fullname) > 1:
                instrument_fullname = instrument_fullname[1].strip()
            else:
                instrument_fullname = None
            agency_id = row.css("td:nth-child(2) a ::attr(href)").extract_first()
            if agency_id is not None:
                agency_id = agency_id.strip().split('=', 1)[-1]
            else:
                agency_id = row.css("td:nth-child(2) ::text").extract_first()
            # status = row.css("td:nth-child(3) ::text").extract_first().strip()
            # launch_date = row.css("td:nth-child(4) ::text").extract_first()
            # if launch_date is not None:
            #     launch_date = dateparser.parse(launch_date.strip(), settings=date_parsing_settings)
            # eol_date = row.css("td:nth-child(5) ::text").extract_first()
            # if eol_date is not None:
            #     eol_date = dateparser.parse(eol_date.strip(), settings=date_parsing_settings)
            # applications = row.css("td:nth-child(6) ::text").extract_first().strip()
            # orbit_details = row.css("td:nth-child(8) ::text").extract_first().strip()  # TODO: Save more detailed orbits!
            print(instrument_name, instrument_id, instrument_fullname, agency_id)
            mission = URIRef("http://ceosdb/instrument#" + instrument_id)
            self.g.add((mission, RDFS.label, Literal(instrument_name)))
            self.g.add((mission, RDF.type, CEOSDB_schema.instrumentClass))
            if instrument_fullname is not None:
                self.g.add((mission, CEOSDB_schema.hasFullName, Literal(instrument_fullname)))
            if agency_id.isdigit():
                self.g.add((mission, CEOSDB_schema.builtBy, URIRef("http://ceosdb/agency#" + agency_id)))
            else:
                self.g.add((mission, CEOSDB_schema.builtBy, Literal(agency_id)))
            # self.g.add((mission, CEOSDB_schema.hasStatus, Literal(status)))
            # if launch_date is not None:
            #     self.g.add((mission, CEOSDB_schema.hasLaunchDate, Literal(launch_date)))
            # if eol_date is not None:
            #     self.g.add((mission, CEOSDB_schema.hasEOLDate, Literal(eol_date)))
            # self.g.add((mission, CEOSDB_schema.hasApplications, Literal(applications)))
            # self.g.add((mission, CEOSDB_schema.hasOrbitDetails, Literal(orbit_details)))

    def define_subclasses(self):
        self.g.add((CEOSDB_schema.agencyClass, RDFS.subClassOf, OWL.Thing))
        self.g.add((CEOSDB_schema.missionClass, RDFS.subClassOf, OWL.Thing))
        self.g.add((CEOSDB_schema.instrumentClass, RDFS.subClassOf, OWL.Thing))
        self.g.add((CEOSDB_schema.measurementCategoryClass, RDFS.subClassOf, OWL.Thing))
        self.g.add((CEOSDB_schema.measurementClass, RDFS.subClassOf, OWL.Thing))

    def closed(self, reason):
        with open('ontology.n3', 'wb') as ont_file:
            ont_file.write(self.g.serialize(format='n3'))
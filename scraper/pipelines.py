# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
from neo4j import GraphDatabase
import os

import scraper.items as items
from sqlalchemy.orm import sessionmaker
from sqlalchemy import or_
from scraper.models import BroadMeasurementCategory, MeasurementCategory, Measurement, \
    Agency, Mission, InstrumentType, GeometryType, Waveband, Instrument, TechTypeMostCommonOrbit, \
    MeasurementMostCommonOrbit, technologies, db_connect, create_tables
from scraper.spiders import CEOSDB_schema
from rdflib import Graph, Literal, RDF, RDFS, URIRef
from rdflib.namespace import FOAF, OWL
from scraper import cypher_tx


class DatabasePipeline(object):
    """Database pipeline for storing scraped items in the database"""
    def __init__(self):
        """
        Initializes database connection and sessionmaker.
        Creates deals table.
        """
        engine = db_connect()
        create_tables(engine)
        self.Session = sessionmaker(bind=engine)

    def fill_instrument_types(self, session, types):
        for instr_type in types:
            instrument_type = InstrumentType(name=instr_type)
            session.add(instrument_type)

    def fill_geometry_types(self, session, geometries):
        for geometry in geometries:
            geometry_type = GeometryType(name=geometry)
            session.add(geometry_type)

    def fill_wavebands(self, session, wavebands):
        for waveband_t in wavebands:
            waveband = Waveband(name=waveband_t[0], wavelengths=waveband_t[1])
            session.add(waveband)

    def add_measurement_category(self, session):
        broad_other = BroadMeasurementCategory(id=1000, name='Other', description='Other')
        session.add(broad_other)
        cat_other = MeasurementCategory(id=1000, name='Other', description='Other', broad_measurement_category_id=1000)
        session.add(cat_other)

    def check_confidences(self, missions_count, missions_param_count, missions_intersect_count):
        if missions_count != 0:
            supp = float(missions_intersect_count) / missions_count
        else:
            supp = 0.
        if missions_param_count != 0:
            conf_param_impl_orbit = float(missions_intersect_count) / missions_param_count
        else:
            conf_param_impl_orbit = 0.
        return supp > 10.0/missions_count and conf_param_impl_orbit > 0.5

    def compute_common_orbit(self, session, param_query):
        most_common_orbit = None
        missions_count = session.query(Mission).filter(Mission.orbit_type != None).filter(
            Mission.orbit_type != 'TBD').count()
        missions_param_count = param_query.count()
        ## First level nodes
        # GEO
        missions_intersect_count = param_query.filter(Mission.orbit_type == 'Geostationary').count()
        if self.check_confidences(missions_count, missions_param_count, missions_intersect_count):
            most_common_orbit = 'GEO'
        # LEO
        missions_orbit_query = session.query(Mission).filter(or_(Mission.orbit_type == 'Inclined, non-sun-synchronous',
                                                                 Mission.orbit_type == 'Sun-synchronous'))
        missions_intersect_query = param_query.filter(or_(Mission.orbit_type == 'Inclined, non-sun-synchronous',
                                                                 Mission.orbit_type == 'Sun-synchronous'))
        missions_intersect_count = missions_intersect_query.count()
        if self.check_confidences(missions_count, missions_param_count, missions_intersect_count):
            most_common_orbit = 'LEO'
        # HEO
        missions_intersect_count = param_query.filter(Mission.orbit_type == 'Highly elliptical').count()
        if self.check_confidences(missions_count, missions_param_count, missions_intersect_count):
            most_common_orbit = 'HEO'

        # In case of LEO, specialize by SSO or other inclined orbits
        if most_common_orbit == 'LEO':
            most_common_orbit_add = ''
            # SSO
            missions_intersect_query = param_query.filter(Mission.orbit_type == 'Sun-synchronous')
            missions_intersect_count = missions_intersect_query.count()
            if self.check_confidences(missions_count, missions_param_count, missions_intersect_count):
                most_common_orbit_add = '-SSO'
            # Prevent SSO form turning into NearPo
            if most_common_orbit_add != '-SSO':
                # Equatorial
                missions_intersect_query = param_query.filter(Mission.orbit_inclination_class == 'Equatorial')
                missions_intersect_count = missions_intersect_query.count()
                if self.check_confidences(missions_count, missions_param_count, missions_intersect_count):
                    most_common_orbit_add = '-Eq'
                # Near Equatorial
                missions_intersect_query = param_query.filter(Mission.orbit_inclination_class == 'Near Equatorial')
                missions_intersect_count = missions_intersect_query.count()
                if self.check_confidences(missions_count, missions_param_count, missions_intersect_count):
                    most_common_orbit_add = '-NearEq'
                # Mid Latitude
                missions_intersect_query = param_query.filter(Mission.orbit_inclination_class == 'Mid Latitude')
                missions_intersect_count = missions_intersect_query.count()
                if self.check_confidences(missions_count, missions_param_count, missions_intersect_count):
                    most_common_orbit_add = '-MidLat'
                # Near Polar
                missions_intersect_query = param_query.filter(Mission.orbit_inclination_class == 'Near Polar')
                missions_intersect_count = missions_intersect_query.count()
                if self.check_confidences(missions_count, missions_param_count, missions_intersect_count):
                    most_common_orbit_add = '-NearPo'
                # Polar
                missions_intersect_query = param_query.filter(Mission.orbit_inclination_class == 'Polar')
                missions_intersect_count = missions_intersect_query.count()
                if self.check_confidences(missions_count, missions_param_count, missions_intersect_count):
                    most_common_orbit_add = '-Po'
            most_common_orbit += most_common_orbit_add

            # Try to specialize for LST
            if most_common_orbit_add == '-SSO':
                most_common_orbit_add = ''
                # DD
                missions_intersect_query = param_query.filter(Mission.orbit_LST_class == 'DD')
                missions_intersect_count = missions_intersect_query.count()
                if self.check_confidences(missions_count, missions_param_count, missions_intersect_count):
                    most_common_orbit_add = '-DD'
                # AM
                missions_intersect_query = param_query.filter(Mission.orbit_LST_class == 'AM')
                missions_intersect_count = missions_intersect_query.count()
                if self.check_confidences(missions_count, missions_param_count, missions_intersect_count):
                    most_common_orbit_add = '-AM'
                # Noon
                missions_intersect_query = param_query.filter(Mission.orbit_LST_class == 'Noon')
                missions_intersect_count = missions_intersect_query.count()
                if self.check_confidences(missions_count, missions_param_count, missions_intersect_count):
                    most_common_orbit_add = '-Noon'
                # PM
                missions_intersect_query = param_query.filter(Mission.orbit_LST_class == 'PM')
                missions_intersect_count = missions_intersect_query.count()
                if self.check_confidences(missions_count, missions_param_count, missions_intersect_count):
                    most_common_orbit_add = '-PM'
                most_common_orbit += most_common_orbit_add

            # Specialize for Orbit Altitude only if already specialized from LEO
            if most_common_orbit != 'LEO':
                most_common_orbit_add = ''
                # VL
                missions_intersect_query = param_query.filter(Mission.orbit_altitude_class == 'VL')
                missions_intersect_count = missions_intersect_query.count()
                if self.check_confidences(missions_count, missions_param_count, missions_intersect_count):
                    most_common_orbit_add = '-VL'
                # L
                missions_intersect_query = param_query.filter(Mission.orbit_altitude_class == 'L')
                missions_intersect_count = missions_intersect_query.count()
                if self.check_confidences(missions_count, missions_param_count, missions_intersect_count):
                    most_common_orbit_add = '-L'
                # M
                missions_intersect_query = param_query.filter(Mission.orbit_altitude_class == 'M')
                missions_intersect_count = missions_intersect_query.count()
                if self.check_confidences(missions_count, missions_param_count, missions_intersect_count):
                    most_common_orbit_add = '-M'
                # H
                missions_intersect_query = param_query.filter(Mission.orbit_altitude_class == 'H')
                missions_intersect_count = missions_intersect_query.count()
                if self.check_confidences(missions_count, missions_param_count, missions_intersect_count):
                    most_common_orbit_add = '-H'
                # VH
                missions_intersect_query = param_query.filter(Mission.orbit_altitude_class == 'VH')
                missions_intersect_count = missions_intersect_query.count()
                if self.check_confidences(missions_count, missions_param_count, missions_intersect_count):
                    most_common_orbit_add = '-VH'
                most_common_orbit += most_common_orbit_add

            # Specialize for repeat cycle only if already specialized for OA
            if most_common_orbit_add == '-VL' or most_common_orbit_add == '-L' or most_common_orbit_add == '-M' \
                    or most_common_orbit_add == '-H' or most_common_orbit_add == '-VH':
                most_common_orbit_add = ''
                # NRC
                missions_intersect_query = param_query.filter(Mission.repeat_cycle_class == None)
                missions_intersect_count = missions_intersect_query.count()
                if self.check_confidences(missions_count, missions_param_count, missions_intersect_count):
                    most_common_orbit_add = '-NRC'
                # SRC
                missions_intersect_query = param_query.filter(Mission.repeat_cycle_class == 'Short')
                missions_intersect_count = missions_intersect_query.count()
                if self.check_confidences(missions_count, missions_param_count, missions_intersect_count):
                    most_common_orbit_add = '-SRC'
                # LRC
                missions_intersect_query = param_query.filter(Mission.repeat_cycle_class == 'Long')
                missions_intersect_count = missions_intersect_query.count()
                if self.check_confidences(missions_count, missions_param_count, missions_intersect_count):
                    most_common_orbit_add = '-LRC'
                most_common_orbit += most_common_orbit_add
        return most_common_orbit

    def compute_common_orbits(self, session):
        # For each technology and type, compute the innermost node on the decision tree that fits all confidence values
        # to be considered a common orbit
        for technology in technologies:
            mission_query = session.query(Mission).join(Instrument, Mission.instruments).filter(Instrument.technology == technology)
            most_common_orbit = self.compute_common_orbit(session, mission_query)
            tt_mco = TechTypeMostCommonOrbit(techtype=technology, orbit=most_common_orbit)
            print(technology, most_common_orbit)
            session.add(tt_mco)
        for type in session.query(InstrumentType).all():
            mission_query = session.query(Mission).join(Instrument, Mission.instruments).filter(Instrument.types.any(InstrumentType.name == type.name))
            most_common_orbit = self.compute_common_orbit(session, mission_query)
            tt_mco = TechTypeMostCommonOrbit(techtype=type.name, orbit=most_common_orbit)
            print(type.name, most_common_orbit)
            session.add(tt_mco)
        for measurement in session.query(Measurement).all():
            mission_query = session.query(Mission).join(Instrument, Mission.instruments).filter(Instrument.measurements.any(Measurement.name == measurement.name))
            most_common_orbit = self.compute_common_orbit(session, mission_query)
            meas_mco = MeasurementMostCommonOrbit(measurement=measurement.name, orbit=most_common_orbit)
            print(measurement.name, most_common_orbit)
            session.add(meas_mco)

    def open_spider(self, spider):
        session = self.Session()

        try:
            for instrument in session.query(Instrument):
                session.delete(instrument)
            for mission in session.query(Mission):
                session.delete(mission)
            for agency in session.query(Agency):
                session.delete(agency)
            for measurement in session.query(Measurement):
                session.delete(measurement)
            for category in session.query(MeasurementCategory):
                session.delete(category)
            for broad_category in session.query(BroadMeasurementCategory):
                session.delete(broad_category)
            for instrument_type in session.query(InstrumentType):
                session.delete(instrument_type)
            for geometry_type in session.query(GeometryType):
                session.delete(geometry_type)
            for waveband in session.query(Waveband):
                session.delete(waveband)
            for tt_mco in session.query(TechTypeMostCommonOrbit):
                session.delete(tt_mco)
            for meas_mco in session.query(MeasurementMostCommonOrbit):
                session.delete(meas_mco)
            self.fill_instrument_types(session, spider.instrument_types)
            self.fill_geometry_types(session, spider.instrument_geometries)
            self.fill_wavebands(session, spider.wavebands)
            self.add_measurement_category(session)
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()

    def process_item(self, item, spider):
        """Save items in the database.

        This method is called for every item pipeline component.

        """
        session = self.Session()

        if isinstance(item, items.BroadMeasurementCategory):
            db_object = BroadMeasurementCategory(**item)
        elif isinstance(item, items.MeasurementCategory):
            db_object = MeasurementCategory(**item)
        elif isinstance(item, items.Measurement):
            db_object = Measurement(**item)
        elif isinstance(item, items.Agency):
            db_object = Agency(**item)
        elif isinstance(item, items.Mission):
            db_object = Mission(id=item['id'], name=item['name'], full_name=item['full_name'], status=item['status'],
                                launch_date=item['launch_date'], eol_date=item['eol_date'],
                                applications=item['applications'], orbit_type=item['orbit_type'],
                                orbit_period=item['orbit_period'], orbit_sense=item['orbit_sense'],
                                orbit_inclination=item['orbit_inclination'],
                                orbit_inclination_num=item['orbit_inclination_num'],
                                orbit_inclination_class=item['orbit_inclination_class'],
                                orbit_altitude=item['orbit_altitude'],
                                orbit_altitude_num=item['orbit_altitude_num'],
                                orbit_altitude_class=item['orbit_altitude_class'],
                                orbit_longitude=item['orbit_longitude'], orbit_LST=item['orbit_LST'],
                                orbit_LST_time=item['orbit_LST_time'], orbit_LST_class=item['orbit_LST_class'],
                                repeat_cycle=item['repeat_cycle'], repeat_cycle_num=item['repeat_cycle_num'],
                                repeat_cycle_class=item['repeat_cycle_class'])
            for agency_id in item['agencies']:
                agency = session.query(Agency).get(agency_id)
                db_object.agencies.append(agency)
        elif isinstance(item, items.Instrument):
            db_object = Instrument(id=item['id'], name=item['name'], full_name=item['full_name'], status=item['status'],
                                   maturity=item['maturity'], technology=item['technology'], sampling=item['sampling'],
                                   data_access=item['data_access'], data_format=item['data_format'],
                                   measurements_and_applications=item['measurements_and_applications'],
                                   resolution_summary=item['resolution_summary'],
                                   best_resolution=item['best_resolution'], swath_summary=item['swath_summary'],
                                   max_swath=item['max_swath'], accuracy_summary=item['accuracy_summary'],
                                   waveband_summary=item['waveband_summary'])
            for agency_id in item['agencies']:
                agency = session.query(Agency).get(agency_id)
                db_object.agencies.append(agency)
            for instr_type in item['types']:
                instrument_type = session.query(InstrumentType).filter(InstrumentType.name == instr_type).first()
                db_object.types.append(instrument_type)
            for geometry in item['geometries']:
                instrument_geometry = session.query(GeometryType).filter(GeometryType.name == geometry).first()
                db_object.geometries.append(instrument_geometry)
            for mission_id in item['missions']:
                mission = session.query(Mission).get(mission_id)
                db_object.missions.append(mission)
            for measurement_id in item['measurements']:
                measurement = session.query(Measurement).get(measurement_id)
                db_object.measurements.append(measurement)
            for waveband in item['wavebands']:
                instrument_waveband = session.query(Waveband).filter(Waveband.name == waveband).first()
                db_object.wavebands.append(instrument_waveband)
        else:
            db_object = None

        try:
            session.add(db_object)
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()

        return item

    def close_spider(self, spider):
        session = self.Session()

        try:
            # Process the orbit data to generate most common orbit data
            self.compute_common_orbits(session)
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()


class GraphPipeline(object):
    """Neo4J pipeline for storing scraped items in a graph database"""

    def __init__(self):
        """
        Initializes Bolt connection to Neo4J
        """
        uri = "bolt://" + os.environ['NEO4J_HOST'] + ":" + os.environ['NEO4J_PORT']
        # Encryption messes with docker container netowrking
        self.driver = GraphDatabase.driver(uri, auth=(os.environ['NEO4J_USER'], os.environ['NEO4J_PASSWORD']), encrypted=False)

    def open_spider(self, spider):
        with self.driver.session() as session:
            summary = session.write_transaction(cypher_tx.delete_all_graph)
            print(summary.counters)

    def process_item(self, item, spider):
        """Save items in the database.

        This method is called for every item pipeline component.

        """
        with self.driver.session() as session:
            if isinstance(item, items.BroadMeasurementCategory):
                summary = session.write_transaction(cypher_tx.add_broad_measurement_category, item)
            elif isinstance(item, items.MeasurementCategory):
                summary = session.write_transaction(cypher_tx.add_measurement_category, item)
            elif isinstance(item, items.Measurement):
                summary = session.write_transaction(cypher_tx.add_measurement, item)
            elif isinstance(item, items.Agency):
                summary = session.write_transaction(cypher_tx.add_agency, item)
            elif isinstance(item, items.Mission):
                summary = session.write_transaction(cypher_tx.add_mission, item)
            elif isinstance(item, items.Instrument):
                summary = session.write_transaction(cypher_tx.add_instrument, item)
            else:
                summary = None

            if summary is not None:
                print(summary.counters)
            return item

    def close_spider(self, spider):
        with self.driver.session() as session:
            pass
            # Process the orbit data to generate most common orbit data
            # self.compute_common_orbits(session)


class OntologyPipeline(object):
    """Ontology pipeline for storing scraped items in an ontology"""
    def __init__(self):
        """
        Initializes database connection and sessionmaker.
        Creates deals table.
        """
        self.g = Graph()
        self.define_subclasses()


    def define_subclasses(self):
        self.g.add((CEOSDB_schema.agencyClass, RDFS.subClassOf, OWL.Thing))
        self.g.add((CEOSDB_schema.missionClass, RDFS.subClassOf, OWL.Thing))
        self.g.add((CEOSDB_schema.instrumentClass, RDFS.subClassOf, OWL.Thing))
        self.g.add((CEOSDB_schema.measurementBroadCategoryClass, RDFS.subClassOf, OWL.Thing))
        self.g.add((CEOSDB_schema.measurementCategoryClass, RDFS.subClassOf, OWL.Thing))
        self.g.add((CEOSDB_schema.measurementClass, RDFS.subClassOf, OWL.Thing))

    def process_item(self, item, spider):
        """Save items in the database.

        This method is called for every item pipeline component.

        """
        if isinstance(item, items.BroadMeasurementCategory):
            bmc = URIRef("http://ceosdb/broad_category#" + str(item['id']))
            self.g.add((bmc, RDFS.label, Literal(item['name'])))
            self.g.add((bmc, RDF.type, CEOSDB_schema.measurementBroadCategoryClass))
            self.g.add((bmc, CEOSDB_schema.hasDescription, Literal(item['description'])))
        elif isinstance(item, items.MeasurementCategory):
            mc = URIRef("http://ceosdb/category#" + str(item['id']))
            self.g.add((mc, RDFS.label, Literal(item['name'])))
            self.g.add((mc, RDF.type, CEOSDB_schema.measurementCategoryClass))
            self.g.add((mc, CEOSDB_schema.hasDescription, Literal(item['description'])))
            self.g.add((mc, CEOSDB_schema.hasBroadCategory, Literal(item['broad_measurement_category_id'])))
        elif isinstance(item, items.Measurement):
            mc = URIRef("http://ceosdb/measurement#" + str(item['id']))
            self.g.add((mc, RDFS.label, Literal(item['name'])))
            self.g.add((mc, RDF.type, CEOSDB_schema.measurementClass))
            self.g.add((mc, CEOSDB_schema.hasDescription, Literal(item['description'])))
            self.g.add((mc, CEOSDB_schema.hasCategory, Literal(item['measurement_category_id'])))
        elif isinstance(item, items.Agency):
            sa = URIRef("http://ceosdb/agency#" + str(item['id']))
            self.g.add((sa, RDFS.label, Literal(item['name'])))
            self.g.add((sa, RDF.type, CEOSDB_schema.agencyClass))
            self.g.add((sa, CEOSDB_schema.isFromCountry, Literal(item['country'])))
            self.g.add((sa, FOAF.homepage, URIRef(item['website'])))
        elif isinstance(item, items.Mission):
            mission = URIRef("http://ceosdb/mission#" + str(item['id']))
            self.g.add((mission, RDFS.label, Literal(item['name'])))
            self.g.add((mission, RDF.type, CEOSDB_schema.missionClass))
            if item['full_name'] is not None:
                self.g.add((mission, CEOSDB_schema.hasFullName, Literal(item['full_name'])))
            for agency_id in item['agencies']:
                self.g.add((mission, CEOSDB_schema.builtBy, URIRef("http://ceosdb/agency#" + str(agency_id))))
            self.g.add((mission, CEOSDB_schema.hasStatus, Literal(item['status'])))
            if item['launch_date'] is not None:
                self.g.add((mission, CEOSDB_schema.hasLaunchDate, Literal(item['launch_date'])))
            if item['eol_date'] is not None:
                self.g.add((mission, CEOSDB_schema.hasEOLDate, Literal(item['eol_date'])))
            self.g.add((mission, CEOSDB_schema.hasApplications, Literal(item['applications'])))
            if item['orbit_type'] != '':
                self.g.add((mission, CEOSDB_schema.hasOrbitType, Literal(item['orbit_type'])))
            if item['orbit_period'] != '':
                self.g.add((mission, CEOSDB_schema.hasOrbitPeriod, Literal(item['orbit_period'])))
            if item['orbit_sense'] != '':
                self.g.add((mission, CEOSDB_schema.hasOrbitSense, Literal(item['orbit_sense'])))
            if item['orbit_inclination'] != '':
                self.g.add((mission, CEOSDB_schema.hasOrbitInclination, Literal(item['orbit_inclination'])))
            if item['orbit_altitude'] != '':
                self.g.add((mission, CEOSDB_schema.hasOrbitAltitude, Literal(item['orbit_altitude'])))
            if item['orbit_longitude'] != '':
                self.g.add((mission, CEOSDB_schema.hasOrbitLongitude, Literal(item['orbit_longitude'])))
            if item['repeat_cycle'] != '':
                self.g.add((mission, CEOSDB_schema.hasRepeatCycle, Literal(item['repeat_cycle'])))
        elif isinstance(item, items.Instrument):
            instrument = URIRef('http://ceosdb/instrument#' + str(item['id']))
            self.g.add((instrument, RDFS.label, Literal(item['name'])))
            self.g.add((instrument, RDF.type, CEOSDB_schema.instrumentClass))
            if item['full_name'] is not None:
                self.g.add((instrument, CEOSDB_schema.hasFullName, Literal(item['full_name'])))
            for agency_id in item['agencies']:
                self.g.add((instrument, CEOSDB_schema.builtBy, URIRef("http://ceosdb/agency#" + str(agency_id))))
            self.g.add((instrument, CEOSDB_schema.hasStatus, Literal(item['status'])))
            self.g.add((instrument, CEOSDB_schema.hasMaturity, Literal(item['maturity'])))
            for type in item['types']:
                self.g.add((instrument, CEOSDB_schema.isOfType, Literal(type)))
            for geometry in item['geometries']:
                self.g.add((instrument, CEOSDB_schema.hasGeometry, Literal(geometry)))
            self.g.add((instrument, CEOSDB_schema.hasTechnology, Literal(item['technology'])))
            if item['sampling'] is not None:
                self.g.add((instrument, CEOSDB_schema.samples, Literal(item['sampling'])))
            if item['data_access'] is not None:
                self.g.add((instrument, CEOSDB_schema.hasDataAccess, Literal(item['data_access'])))
            if item['data_format'] is not None:
                self.g.add((instrument, CEOSDB_schema.hasDataFormat, Literal(item['data_format'])))
            self.g.add((instrument, CEOSDB_schema.hasMeasurementsSummary, Literal(item['measurements_and_applications'])))
            for mission_id in item['missions']:
                self.g.add((instrument, CEOSDB_schema.isInMission, URIRef("http://ceosdb/mission#" + str(mission_id))))
            for measurement_id in item['measurements']:
                self.g.add((instrument, CEOSDB_schema.isInMission, URIRef("http://ceosdb/measurement#" + str(measurement_id))))
            if item['resolution_summary'] is not None:
                self.g.add((instrument, CEOSDB_schema.hasResolutionSummary, Literal(item['resolution_summary'])))
            if item['best_resolution'] is not None:
                self.g.add((instrument, CEOSDB_schema.hasBestResolution, Literal(item['best_resolution'])))
            if item['swath_summary'] is not None:
                self.g.add((instrument, CEOSDB_schema.hasSwathSummary, Literal(item['swath_summary'])))
            if item['max_swath'] is not None:
                self.g.add((instrument, CEOSDB_schema.hasMaxSwath, Literal(item['max_swath'])))
            if item['accuracy_summary'] is not None:
                self.g.add((instrument, CEOSDB_schema.hasAccuracySummary, Literal(item['accuracy_summary'])))
            if item['waveband_summary'] is not None:
                self.g.add((instrument, CEOSDB_schema.hasWavebandSummary, Literal(item['waveband_summary'])))
            for waveband in item['wavebands']:
                self.g.add((instrument, CEOSDB_schema.hasWaveband, Literal(waveband)))
        return item

    def close_spider(self, spider):
        with open('ontology.n3', 'wb') as ont_file:
            ont_file.write(self.g.serialize(format='n3'))

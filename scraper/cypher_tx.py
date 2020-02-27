from scraper.items import MeasurementCategory, BroadMeasurementCategory, Measurement, Agency, Mission, Instrument


def delete_all_graph(tx):
    return tx.run("MATCH (n)"
                  "DETACH DELETE n").summary()


def add_broad_measurement_category(tx, item: BroadMeasurementCategory):
    return tx.run("CREATE (a:BroadMeasurementCategory {id: {id}, name: {name}, description: {description}})", item).summary()


def add_measurement_category(tx, item: MeasurementCategory):
    tx.run("CREATE (a:MeasurementCategory {id: {id}, name: {name}, description: {description}})", item)
    return tx.run("MATCH (a:BroadMeasurementCategory), (b:MeasurementCategory)"
                  "WHERE a.id = {id1} AND b.id = {id2}"
                  "CREATE (a)-[r1:INCLUDES]->(b)"
                  "CREATE (b)-[r2:TYPE_OF]->(a)", id1=item["broad_measurement_category_id"], id2=item["id"]).summary()


def add_measurement(tx, item: Measurement):
    tx.run("CREATE (a:Measurement {id: {id}, name: {name}, description: {description}})", item)
    return tx.run("MATCH (a:MeasurementCategory), (b:Measurement)"
                  "WHERE a.id = {id1} AND b.id = {id2}"
                  "CREATE (a)-[r1:INCLUDES]->(b)"
                  "CREATE (b)-[r2:TYPE_OF]->(a)", id1=item["measurement_category_id"], id2=item["id"]).summary()


def add_agency(tx, item: Agency):
    return tx.run("CREATE (a:Agency {id: {id}, name: {name}, country: {country}, website: {website}})", item).summary()


def add_mission(tx, item: Mission):
    summary = tx.run("CREATE (a:Mission {id: {id}, name: {name}, full_name: {full_name}, status: {status}, "
                     "launch_date: {launch_date}, eol_date: {eol_date}, applications: {applications},"
                     "orbit_type: {orbit_type}, orbit_period: {orbit_period}, orbit_sense: {orbit_sense},"
                     "orbit_inclination: {orbit_inclination}, orbit_inclination_num: {orbit_inclination_num}, "
                     "orbit_inclination_class: {orbit_inclination_class}, orbit_altitude: {orbit_altitude},"
                     "orbit_altitude_num: {orbit_altitude_num}, orbit_altitude_class: {orbit_altitude_class},"
                     "orbit_longitude: {orbit_longitude}, orbit_LST: {orbit_LST}, orbit_LST_time: {orbit_LST_time},"
                     "orbit_LST_class: {orbit_LST_class}, repeat_cycle: {repeat_cycle},"
                     "repeat_cycle_num: {repeat_cycle_num}, repeat_cycle_class: {repeat_cycle_class}})", item).summary()
    for agency_id in item['agencies']:
        rel_sum = tx.run("MATCH (a:Mission), (b:Agency)"
                         "WHERE a.id = {id1} AND b.id = {id2}"
                         "CREATE (a)-[r1:BUILT_BY]->(b)"
                         "CREATE (b)-[r2:BUILT]->(a)", id1=item["id"], id2=agency_id).summary()
    return summary

def add_instrument(tx, item: Instrument):
    summary = tx.run("CREATE (a:Instrument {id: {id}, name: {name}, full_name: {full_name}, status: {status}, "
                     "maturity: {maturity}, technology: {technology}, sampling: {sampling}, data_access: {data_access},"
                     "data_format: {data_format}, measurements_and_applications: {measurements_and_applications},"
                     "resolution_summary: {resolution_summary}, best_resolution: {best_resolution}, "
                     "swath_summary: {swath_summary}, max_swath: {max_swath}, accuracy_summary: {accuracy_summary},"
                     "waveband_summary: {waveband_summary}, types: {types}, geometries: {geometries}, "
                     "wavebands: {wavebands}})", item).summary()
    for agency_id in item['agencies']:
        rel_sum = tx.run("MATCH (a:Instrument), (b:Agency)"
                         "WHERE a.id = {id1} AND b.id = {id2}"
                         "CREATE (a)-[r1:BUILT_BY]->(b)"
                         "CREATE (b)-[r2:BUILT]->(a)", id1=item["id"], id2=agency_id).summary()
        print(rel_sum.counters)
    for mission_id in item['missions']:
        rel_sum = tx.run("MATCH (a:Instrument), (b:Mission)"
                         "WHERE a.id = {id1} AND b.id = {id2}"
                         "CREATE (a)-[r1:CARRIED_BY]->(b)"
                         "CREATE (b)-[r2:CARRIES]->(a)", id1=item["id"], id2=mission_id).summary()
        print(rel_sum.counters)
    for measurement_id in item['measurements']:
        rel_sum = tx.run("MATCH (a:Instrument), (b:Measurement)"
                         "WHERE a.id = {id1} AND b.id = {id2}"
                         "CREATE (a)-[r1:MEASURES]->(b)", id1=item["id"], id2=measurement_id).summary()
        print(rel_sum.counters)
    return summary
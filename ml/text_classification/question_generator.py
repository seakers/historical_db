from string import Template
import os
import random
from sqlalchemy.orm import sessionmaker
from scraper.models import BroadMeasurementCategory, MeasurementCategory, Measurement, \
    Agency, Mission, InstrumentType, GeometryType, Waveband, Instrument, db_connect, create_tables

# parameters
NUM_QUESTIONS_PER_CLASS = 1000

# Connect to the database to retrieve names
engine = db_connect()
Session = sessionmaker(bind=engine)

# Define template substitutions depending on the type
substitutions = dict()

def subs_measurement(session):
    measurements = session.query(Measurement).all()
    return random.choice(measurements).name

def subs_year(session):
    return random.randrange(1965, 2055)

substitutions['measurement'] = subs_measurement
substitutions['year'] = subs_year

# Iterate over all types of questions
for filename in os.listdir('./question_templates'):
    question_class = int(filename.split('.', 1)[0])
    parameter_map = {}
    template_lines = []
    session = Session()

    with open('./question_templates/' + filename, 'r') as file:
        separator = False
        for line in file:
            if separator:
                # Add to list of templates
                template_lines.append(Template(line[:-1]))
            else:
                # Add to list of variables
                if line == '--\n':
                    separator = True
                else:
                    line_info = line.split()
                    parameter_map[line_info[0]] = line_info[1]

    # Start generating random questions
    if not os.path.exists('./data'):
        os.makedirs('./data')
    with open('./data/' + filename, 'w') as file:
        for i in range(1, NUM_QUESTIONS_PER_CLASS+1):
            # Generate a set of parameters
            params = {}
            for param, type in parameter_map.items():
                params[param] = substitutions[type](session)

            # Generate a question
            template = random.choice(template_lines)
            question = template.substitute(params)
            file.write(question + '\n')
            print(question)
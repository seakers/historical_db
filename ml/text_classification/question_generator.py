from string import Template
import os
import random
from sqlalchemy.orm import sessionmaker
import scraper.models as models

# Connect to the database to retrieve names
engine = models.db_connect()
Session = sessionmaker(bind=engine)

# Define template substitutions depending on the type
substitutions = dict()

def subs_measurement(session):
    measurements = session.query(models.Measurement).all()
    return random.choice(measurements).name

def subs_technology(session):
    technologies = list(models.technologies)
    for type in session.query(models.InstrumentType).all():
        technologies.append(type.name)
    return random.choice(technologies)

def subs_mission(session):
    missions = session.query(models.Mission).all()
    return random.choice(missions).name

def subs_year(session):
    return random.randrange(1965, 2055)

substitutions['measurement'] = subs_measurement
substitutions['technology'] = subs_technology
substitutions['mission'] = subs_mission
substitutions['year'] = subs_year

# Iterate over all types of questions
for filename in os.listdir('./question_templates'):
    question_class = int(filename.split('.', 1)[0])
    parameter_map = {}
    template_lines = []
    num_questions = 0
    labels = ""
    session = Session()

    with open('./question_templates/' + filename, 'r') as file:
        state = 1
        for line in file:
            if line == '--\n':
                state += 1
            else:
                if state == 1:
                    # Set number of questions
                    num_questions = int(line[:-1])
                elif state == 2:
                    # Add to list of variables
                    line_info = line.split()
                    parameter_map[line_info[0]] = line_info[1]
                elif state == 3:
                    # Add to list of templates
                    labels = line[:-1]
                elif state == 4:
                    # Add to list of templates
                    template_lines.append(Template(line[:-1]))


    # Start generating random questions
    if not os.path.exists('./data'):
        os.makedirs('./data')
    with open('./data/' + filename, 'w') as file:
        file.write(labels + "\n")
        for i in range(1, num_questions+1):
            # Generate a set of parameters
            params = {}
            for param, type in parameter_map.items():
                params[param] = substitutions[type](session)

            # Generate a question
            template = random.choice(template_lines)
            question = template.substitute(params)
            file.write(question + '\n')
            print(question)
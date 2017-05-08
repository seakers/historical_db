# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

from scrapy.item import Item, Field


class BroadMeasurementCategory(Item):
    """Defines a measurement broad category"""
    id = Field()
    name = Field()
    description = Field()


class MeasurementCategory(Item):
    """Defines a measurement category"""
    id = Field()
    name = Field()
    description = Field()
    broad_measurement_category_id = Field()


class Measurement(Item):
    """Defines a measurement"""
    id = Field()
    name = Field()
    description = Field()
    measurement_category_id = Field()


class Agency(Item):
    """Defines a Space Agency"""
    id = Field()
    name = Field()
    country = Field()
    website = Field()


class Mission(Item):
    """Defines an EO mission"""
    id = Field()
    name = Field()
    full_name = Field()
    agencies = Field()
    status = Field()
    launch_date = Field()
    eol_date = Field()
    applications = Field()
    orbit_type = Field()
    orbit_period = Field()
    orbit_sense = Field()
    orbit_inclination = Field()
    orbit_altitude = Field()
    orbit_longitude = Field()
    orbit_LST = Field()
    repeat_cycle = Field()


class Instrument(Item):
    """Defines an EO Instrument"""
    id = Field()
    name = Field()
    full_name = Field()
    status = Field()
    agencies = Field()
    maturity = Field()
    types = Field()
    geometries = Field()
    technology = Field()
    sampling = Field()
    data_access = Field()
    data_format = Field()
    measurements_and_applications = Field()
    missions = Field()
    measurements = Field()
    resolution_summary = Field()
    best_resolution = Field()
    swath_summary = Field()
    max_swath = Field()
    accuracy_summary = Field()
    waveband_summary = Field()
# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

from scrapy.item import Item, Field


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
    agency_id = Field()

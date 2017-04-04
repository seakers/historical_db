# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import scraper.items as items
from sqlalchemy.orm import sessionmaker
from scraper.models import Agency, Mission, db_connect, create_tables


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

    def open_spider(self, spider):
        session = self.Session()

        try:
            session.query(Agency).delete()
            session.query(Mission).delete()
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()

    def process_item(self, item, spider):
        """Save deals in the database.

        This method is called for every item pipeline component.

        """
        session = self.Session()

        if isinstance(item, items.Agency):
            object = Agency(**item)
        elif isinstance(item, items.Mission):
            object = Mission(**item)
        else:
            object = None

        try:
            session.add(object)
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()

        return item

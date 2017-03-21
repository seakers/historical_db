import re
import scrapy

class CEOSDBSpider(scrapy.Spider):
    name = "ceosdb_spider"
    start_urls = ['http://database.eohandbook.com/database/agencytable.aspx',
            'http://database.eohandbook.com/database/missiontable.aspx',
            'http://database.eohandbook.com/database/instrumenttable.aspx']

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
        TR_SELECTOR = "#dgAgencies tr"
        for row in response.css(TR_SELECTOR)[1:]:
            agency = row.css("td:nth-child(1) b a ::text").extract_first().strip()
            country = row.css("td:nth-child(2) ::text").extract_first().strip()
            website = row.css("td:nth-child(3) a ::attr(href)").extract_first().strip()
            num_missions = row.css("td:nth-child(4) ::text").extract_first().strip()
            num_missions = re.match(r"\d*", num_missions).group(0)
            num_instruments = row.css("td:nth-child(5) ::text").extract_first().strip().replace("-", "")
            print(agency, country, website, num_missions, num_instruments)

    def parse_missions(self, response):
        return None

    def parse_instruments(self, response):
        return None

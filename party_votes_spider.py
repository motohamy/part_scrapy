import scrapy
from csu_scraper.items import MunicipalityPartyVotes

class PartyVotesSpider(scrapy.Spider):
    name = "party_votes_spider"

    years = [2008, 2012, 2016, 2020]
    cz_nuts_codes = open("../NUTS.txt").readlines()

    def start_requests(self):
        for year in self.years:
            for cz_nuts_code in self.cz_nuts_codes:
                yield scrapy.Request(
                    url=f'https://www.volby.cz/pls/kz{year}/vysledky_okres?nuts={cz_nuts_code}',
                    callback=self.parse,
                    meta={
                        'year': year
                    }
                )

    def parse(self, response):
        year = response.meta.get('year')

        response.selector.remove_namespaces()


        for municipality in response.xpath("//OBEC"):
            

            zuj_code = municipality.xpath('./@CIS_OBEC').get()
            municipality_name = municipality.xpath('./@NAZ_OBEC').get()

            for vote in municipality.xpath('./HLASY_STRANA'):
                votes_per_party = MunicipalityPartyVotes()
                votes_per_party['zuj_code'] = zuj_code
                votes_per_party['municipality_name'] = municipality_name
                votes_per_party['year'] = year
                votes_per_party['party_code'] = vote.xpath('./@KSTRANA').get()
                votes_per_party['votes_count'] = vote.xpath('./@HLASY').get()
                votes_per_party['votes_percentage'] = vote.xpath('./@PROC_HLASU').get()

                yield votes_per_party
    

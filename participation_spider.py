import scrapy
import re
from csu_scraper.items import MunicipalityParticipation


class ParticipationSpider(scrapy.Spider):
    name = "participation_spider"

    years = [2008, 2012, 2016, 2020]

    def start_requests(self):
        yield scrapy.Request(
            url=f'https://www.volby.cz',
            callback=self.parse_overview,
        )

    def parse_overview(self, response):

        election_year_links = response.xpath(
            '//td[text()="Zastupitelstva krajů"]/following::td[1]/a[position()>2]/@href').getall()

        years = response.xpath(
            '//td[text()="Zastupitelstva krajů"]/following::td[1]/a[position()>2]/text()').getall()

        if(len(years) == len(election_year_links)):
            for i, _ in enumerate(election_year_links):
                yield scrapy.Request(
                    # In every case is third option from menu
                    url=f'{response.request.url}{election_year_links[i].replace("?", "3?")}',
                    callback=self.parse_result_by_territory,
                    meta={
                        'year': int(years[i])
                    }
                )
        else:
            raise Exception(
                "Různá délka polí years a election_year_links")

    def parse_result_by_territory(self, response):
        year = response.meta.get('year')

        base_url = re.match(
            r'(^.*kz\d{4}/).*', response.request.url).group(1)

        nuts_codes = response.xpath(
            "//tr/td[contains(@headers, 'sa1')]/a/text()").getall()

        municipalities_links = response.xpath(
            "//tr/td[contains(@headers, 'sa3')]/a/@href").getall()

        if(len(nuts_codes) == len(municipalities_links)):
            for municipality in zip(nuts_codes, municipalities_links):

                yield scrapy.Request(
                    # In every case is third option from menu
                    url=f"{base_url}{municipality[1]}",
                    callback=self.parse_municipalities,
                    meta={
                        'base_url': base_url,
                        'year': year,
                        'nuts_code': municipality[0],
                    }
                )
        else:
            raise Exception(
                "Různá délka polí nuts_codes a municipalites_links")

    def parse_municipalities(self, response):
        base_url = response.meta.get('base_url')
        year = response.meta.get('year')
        nuts_code = response.meta.get('nuts_code')

        zuj_selector = response.xpath(
            "//tr/td[contains(@headers, 'sa1')]/a")

        zuj_codes = zuj_selector.xpath("./text()").getall()

        parties_list_links = zuj_selector.xpath("./@href").getall()

        if(len(zuj_codes) == len(parties_list_links)):
            for parties in zip(zuj_codes, parties_list_links):

                yield scrapy.Request(
                    url=f"{base_url}{parties[1]}",
                    callback=self.parse_parties_list,
                    meta={
                        'year': year,
                        'nuts_code': nuts_code,
                        'zuj':  parties[0]
                    }
                )
        else:
            raise Exception(
                "Různá délka polí zuj_codes a parties_list_links")

    def parse_parties_list(self, response):
        year = response.meta.get('year')
        nuts_code = response.meta.get('nuts_code')
        zuj = response.meta.get('zuj')

        municipality_participation = MunicipalityParticipation(
            year=year,
            nuts_code=nuts_code,
            zuj_code=zuj,
            valid_voters=int(response.xpath(
                "//tr/td[contains(@headers, 'sa2')]/text()").get().replace('\xa0', '')),
            participation_percentage=float(response.xpath(
                "//tr/td[contains(@headers, 'sa4')]/text()").get().replace(',', '.'))
        )

        yield municipality_participation

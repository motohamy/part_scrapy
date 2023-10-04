import scrapy
import re
from csu_scraper.items import MunicipalityPrefereVotes


class PreferentialVotesSpider(scrapy.Spider):
    """Spider, který bude sloužit pro stahování dat o preferenčních hlasech kandidáta
     """

    name = "preferential_votes_spider"

    years = [2008, 2012, 2016, 2020]
    """list[int]: Roky konání voleb
    """

    def start_requests(self):
        """Vytváří prvotní dotaz na server
        """

        yield scrapy.Request(
            url=f'https://www.volby.cz',
            callback=self.parse_overview,
        )

    def parse_overview(self, response):
        """Zpracovává stránku 'Výsledky referend a voleb' na stránkách ČSÚ k volbám
        Yields:
                scrapy.Request: Dotaz na server Volby.cz, který vrátí výsledky
        """

        election_year_links = response.xpath(
            '//td[text()="Zastupitelstva krajů"]/following::td[1]/a[position()>2]/@href').getall()
        """list[string]: Odkazy na výsledky voleb po roce 2004
        """

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
            for i, municipalility in enumerate(municipalities_links):

                yield scrapy.Request(
                    # In every case is third option from menu
                    url=f"{base_url}/{municipalility}",
                    callback=self.parse_municipalities,
                    meta={
                        'base_url': base_url,
                        'nuts_code': nuts_codes[i],
                        'year': year,
                    }
                )
        else:
            raise Exception(
                "Různá délka polí nuts_codes a municipalites_links")

    def parse_municipalities(self, response):
        base_url = response.meta.get('base_url')
        nuts_code = response.meta.get('nuts_code')
        year = response.meta.get('year')

        zuj_selector = response.xpath(
            "//tr/td[contains(@headers, 'sa1')]/a")

        zuj_codes = zuj_selector.xpath("./text()").getall()

        parties_list_links = zuj_selector.xpath("./@href").getall()

        if(len(zuj_codes) == len(parties_list_links)):
            for i, parties_list in enumerate(parties_list_links):

                yield scrapy.Request(
                    # In every case is third option from menu
                    url=f"{base_url}/{parties_list}",
                    callback=self.parse_parties_list,
                    meta={
                        'base_url': base_url,
                        'nuts_code': nuts_code,
                        'year': year,
                        'zuj_code': zuj_codes[i]
                    }

                )
        else:
            raise Exception(
                "Různá délka polí zuj_codes a parties_list_links")

    def parse_parties_list(self, response):
        base_url = response.meta.get('base_url')
        nuts_code = response.meta.get('nuts_code')
        year = response.meta.get('year')
        zuj_code = response.meta.get('zuj_code')

        party_codes = self.clean_list(response.xpath(
            "//tr/td[contains(@headers, 'sb1')]/text()").getall())[1:]  # KSTRANA

        preferential_votes_links = response.xpath(
            "//tr/td[contains(@headers, 'sa3')]/a/@href").getall()

        if(len(party_codes) == len(preferential_votes_links)):
            for i, preferential_votes in enumerate(preferential_votes_links):

                yield scrapy.Request(
                    # In every case is third option from menu
                    url=f"{base_url}/{preferential_votes}",
                    callback=self.parse_preferential_votes,
                    meta={
                        'nuts_code': nuts_code,
                        'year': year,
                        'zuj_code': zuj_code,
                        'party_code': party_codes[i]
                    }
                )
        else:
            raise Exception(
                f"Různá délka polí party_codes ({len(party_codes)}) a preferential_list_links ({ len(preferential_votes_links)})")

    def parse_preferential_votes(self, response):
        nuts_code = response.meta.get('nuts_code')
        year = response.meta.get('year')
        zuj_code = response.meta.get('zuj_code')
        party_code = response.meta.get('party_code')

        serial_numbers_of_candites = response.xpath(
            "//tr/td[contains(@headers, 'sb1')]/text()").getall()

        preferential_votes = response.xpath(
            "//tr/td[contains(@headers, 'sb4')]/text()").getall()

        percentage_of_preferential_votes = response.xpath(
            "//tr/td[contains(@headers, 'sb5')]/text()").getall()

        if(len(serial_numbers_of_candites) == len(preferential_votes) == len(percentage_of_preferential_votes)):
            for i, _ in enumerate(serial_numbers_of_candites):
                if(preferential_votes[i] != '-' and int(preferential_votes[i].replace('\xa0', '')) != 0):

                    candidate_votes = MunicipalityPreferentialVotes(
                        nuts_code=nuts_code, year=year, zuj_code=zuj_code, party_code=party_code)

                    candidate_votes['preferential_votes'] = int(
                        preferential_votes[i].replace('\xa0', ''))

                    candidate_votes['candite_party_serial_number'] = int(
                        serial_numbers_of_candites[i])

                    candidate_votes['percentage_of_preferential_votes'] = float(
                        percentage_of_preferential_votes[i].replace(',', '.'))

                    yield candidate_votes

        else:
            raise Exception(
                f"Různá délka polí serial_numbers_of_candites ({len(serial_numbers_of_candites)}), preferential_votes ({len(preferential_votes)}), percentage_of_preferential_votes({len(percentage_of_preferential_votes)})")

    def clean_list(self, list):
        try:
            list.remove('-')
            return list
        except:
            return list

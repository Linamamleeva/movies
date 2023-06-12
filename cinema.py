# scrape webpage
import scrapy
from scrapy.crawler import CrawlerProcess
import pandas as pd


class WikiSpider(scrapy.Spider):
    name = 'wikispider'
    start_urls = [
        'https://ru.wikipedia.org/wiki/250_лучших_фильмов_по_версии_IMDb']

    def parse(self, response):
        base_url = 'https://ru.wikipedia.org'
        for movie in response.css('table tr:not(:first-child)'):
            title = movie.css('td:nth-child(2) a::text').get()
            year = movie.css('td:nth-child(3) a::text').get()
            director = movie.css('td:nth-child(4) a::text').get()
            genre = movie.css('td:nth-child(5) a::text').getall()
            link = movie.css('td:nth-child(2) a::attr(href)').get()
            yield response.follow(url=base_url + link, callback=self.parse_movie, cb_kwargs=dict(title=title, year=year, director=director, genre=genre))

    def parse_movie(self, response, title, year, director, genre):
        producer_selector = 'th:contains("Продюсер") + td span::text'
        country_selector = 'th:contains("Страна") + td.plainlist a::text'
        actors_selector = 'th:contains("главных") + td.plainlist a::text'
        length_selector = 'th:contains("Длительность") + td span::text'
        budget_selector = 'th:contains("Бюджет") + td span::text'

        movie_data = {
            'title': title,
            'year': year,
            'director': director,
            'genre': genre,
            'producer': response.css(producer_selector).getall(),
            'country': response.css(country_selector).get(),
            'actors': response.css(actors_selector).getall(),
            'length': response.css(length_selector).get(),
            'budget': response.css(budget_selector).get(),
        }
        
        yield movie_data

process = CrawlerProcess({
    'FEED_FORMAT': 'csv',
    'FEED_URI': 'output.csv'
})
process.crawl(WikiSpider)
process.start()


import scrapy
import os
import json
from scrapy.crawler import CrawlerProcess


def cleanText(text):
    text = text.strip()
    text = text.replace("\n", "")
    text = text.replace("\r", "")
    text = text.replace("\t", "")
    text = text.replace("<p>", "")
    text = text.replace("</p>", "")
    text = text.replace("<strong>", "")
    text = text.replace("</strong>", "")
    text = text.replace("<em>", "")
    text = text.replace("</em>", "")
    text = text.replace('<span style="font-size:14px">', "")
    text = text.replace('</span>', "")
    text = text.replace('<a class=\"text-primary\" href=\"https://sieuthivieclam.vn/dang-nhap.html\">Đăng nhập</a>', "")
    text = text.replace('<p style=\"margin-left:36.0pt\"> * <u>', "")
    text = text.replace('</u>:', "")
    text = text.replace('\"', "")
    return text

class CkSonSpider(scrapy.Spider):
    name = "ck_son"
    start_urls = ["https://sieuthivieclam.vn"]
    page_start = 1
    page_end = 2

    dir = "/opt/airflow/data"
    if not os.path.exists(dir):
        os.makedirs(dir)
    file_path2 = os.path.join(dir, "nguyenvanson_stvl.csv")
    with open(file_path2, 'a', encoding='utf-8') as f:
        line = f"'job_name','company_name','update_date','welfare','job_description'\n"
        f.write(line)

    def start_requests(self):
        for i in range(self.page_start, self.page_end):
            url = f"https://sieuthivieclam.vn/tim-viec-lam/category_2-cong-nghe-thong-tin/trang-{i}.html"
            yield scrapy.Request(url=url, callback=self.parse_page)

    def parse_page(self, response):
        links = response.xpath('//h3[@class="title-job"]/a/@href').getall()
        for link in links:
            link = "https://sieuthivieclam.vn/" + link
            yield scrapy.Request(url=link, callback=self.parse_detail)

    def parse_detail(self, response):
        item = {}

        item['job_name'] = response.xpath('//h1[@class="save-job page-title"]/text()').get()
        item['company_name'] = response.xpath('//h3[@class="company-name"]/text()').get()
        item['update_date'] = response.xpath('//span[@class="post-update"]/text()').get()

        benefit_info = response.xpath('//div[@class="benefit-info"]')
        item['welfare'] = [cleanText(p.get()) for p in benefit_info.xpath('.//p')]

        description_info = response.xpath('//div[@class="description-info"]')
        item['job_description'] = cleanText(description_info.xpath('.//p/text()').get())

        dir = "/opt/airflow/data"
        if not os.path.exists(dir):
            os.makedirs(dir)

        file_path1 = os.path.join(dir, "nguyenvanson_stvl.json")
        file_path2 = os.path.join(dir, "nguyenvanson_stvl.csv")

        with open(file_path1, 'a', encoding='utf-8') as f:
            line = json.dumps(item, ensure_ascii=False) + "\n"
            f.write(line)
        with open(file_path2, 'a', encoding='utf-8') as f:
            line = f"'{item['job_name']}','{item['company_name']}','{item['update_date']}','{item['welfare']}','{item['job_description']}'\n"
            f.write(line)

process = CrawlerProcess(settings={
    "CONCURRENT_REQUESTS": 3,
    "DOWNLOAD_TIMEOUT": 60,
    "RETRY_TIMES": 5,
    "ROBOTSTXT_OBEY": False,
    "USER_AGENT": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0",
})
process.crawl(CkSonSpider)
process.start()

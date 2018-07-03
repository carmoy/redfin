from lxml import html  

file_path = "sold_in30days_from_2018-02-11-23-14-36/1184173"

parser = html.parse(file_path)
search_results = parser.xpath('//*[@id="house-info"]/div/div/div[1]/p/span/text()')

print search_results


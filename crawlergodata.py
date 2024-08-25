from urllib.parse import urlparse
import validators

if __name__ == '__main__':
    with open('crawler.txt', 'r') as f:
        crawlerurls = f.read().split()

    with open('pureurls.txt') as f:
        urls = set(f.read().split())

    parsed_urls = set()
    for curl in crawlerurls:
        if not validators.url(curl):
            continue
        parsed_url = urlparse(curl)
        if parsed_url.port is not None:
            parsed_urls.add(parsed_url.hostname + ":" + str(parsed_url.port) + parsed_url.path)
        else:
            parsed_urls.add(parsed_url.hostname + parsed_url.path)

    crawlergodata = set()
    for url in urls:

        not_in = True
        for curl in parsed_urls:
            if curl in url:
                not_in = False
                break
        if not_in:
            if validators.url(url):
                crawlergodata.add(url)

    with open('crawler_continue.txt', 'w') as f:
        f.write('\n'.join(crawlergodata))

# See PyCharm help at https://www.jetbrains.com/help/pycharm/

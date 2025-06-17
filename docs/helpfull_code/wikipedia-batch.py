import requests
import json

def fetch_wikipedia_pages(titles):
    session = requests.Session()
    url = "https://de.wikipedia.org/w/api.php"
    params = {
        'action': 'query',
        'format': 'json',
        'titles': "|".join(titles),
        'prop': 'extracts|categories|langlinks|info',
        'exintro': True,
        'explaintext': True,
        'cllimit': 'max',
        'lllimit': 'max',
        'redirects': True,
        'inprop': 'url'           # liefert fullurl
    }
    resp = session.get(url, params=params).json()
    result = {}

    for page in resp['query'].get('pages', {}).values():
        title_de = page.get('title')

        # 1) Labels
        label_de = title_de                                     # Quelle: title
        # englisches Label, falls vorhanden, sonst Fallback auf deutsch
        langlinks_all = {ll['lang']: ll['*'] for ll in page.get('langlinks', [])}
        label_en = langlinks_all.get('en', label_de)            # Quelle: langlinks

        # 2) Extract
        extract_from_extracts = page.get('extract', "")         # Quelle: extracts

        # 3) Kategorien
        categories_from_categories = [c['title']
                                      for c in page.get('categories', [])]  # Quelle: categories

        # 4) Normale Sitelinks (= alle Interwiki-Sprachversionen)
        sitelinks_all = langlinks_all                          # Quelle: langlinks

        # 5) Vollständige Seiten-URL
        full_url_from_info = page.get('fullurl')               # Quelle: info → fullurl

        result[title_de] = {
            'label_de (title)': label_de,
            'label_en (langlinks[en])': label_en,
            'extract (extracts)': extract_from_extracts,
            'categories (categories)': categories_from_categories,
            'langlinks_all (langlinks)': sitelinks_all,
            'fullurl (info)': full_url_from_info
        }

    return result

if __name__ == "__main__":
    titles = ["Berlin", "München", "Köln"]
    data = fetch_wikipedia_pages(titles)
    print(json.dumps(data, ensure_ascii=False, indent=2))

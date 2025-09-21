#!/usr/bin/env python3
"""Test URL replacement manually"""
from bs4 import BeautifulSoup

html = '''<img src="https://image.rakuten.co.jp/tsutsu-uraura/cabinet/productpic/yufu.jpg" alt="test">'''

print("Original:", html)

soup = BeautifulSoup(html, 'html.parser')
img = soup.find('img')

if img and img.get('src'):
    old_url = img['src']
    filename = old_url.split('/')[-1]
    new_url = f"https://cdn.shopify.com/s/files/1/0637/6059/7127/files/{filename}?v=1758179452"
    img['src'] = new_url
    print("New:", str(soup))
    print("Filename:", filename)
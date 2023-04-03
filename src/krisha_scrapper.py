import re
import os
import time
import random
import requests
import csv
from bs4 import BeautifulSoup
from src.adls_ingestion import write_to_ADLS
from dotenv import load_dotenv


# This app scraps krisha.kz website, specifically the information of appartment for rent
# posts responding to the following filtration conditions:
# 1. contains pictures
# 2. not the first floor


def get_request(url, headers, card_class):
    # Returns an html text of the request
    req = requests.get(url=url, headers=headers)
    soup = BeautifulSoup(req.text, 'lxml')
    appartment_cards = soup.find_all('div', class_=re.compile(card_class))
    return appartment_cards


def parse_header(header_class, app_card) -> list:
    # Parses the header of post
    header = app_card.find('a', class_=header_class).text\
                            .replace(' м²', '')\
                            .replace(' этаж помесячно', '')\
                            .replace('-комнатная квартира', '')\
                            .split(', ')
    return header


def parse_address(address_class, app_card) -> str:
    # Parses the address of post
    address = app_card.find('div', class_=address_class).text\
                            .strip()\
                            .replace(' р-н', '')\
                            .split(', ')
    if len(address) > 2:
        address = [address[0], ' '.join(address[1:])]
    if len(address) == 1:
        address = ['', address[0]]
    return address


def parse_price(price_class, app_card) -> str:
    # Parses the price of post
    price = app_card.find('div', class_=price_class).text \
                            .replace("\xa0", '') \
                            .replace('〒', '') \
                            .strip()
    return price

def parse_owner(owner_class, app_card) -> str:
    # Parses the type of owner of post
    owner = app_card.find('div', class_=re.compile(owner_class)).text \
                            .strip()
    return owner

def parse_date(date_class, app_card, month_string) -> str:
    # Parses the date of post
    date = app_card.find('div', {'class': date_class}, string=re.compile(month_string))\
                            .text\
                            .strip()
    return date

def parse_url(app_card):
    # Parses the url of post
    url = 'krisha.kz' + app_card.find('a').get('href')
    return url


def writer(path_to_write, row):
    # Writes data to a csv file row by row
    with open(path_to_write, 'a', newline='', encoding='utf-8') as file:
        writer = csv.writer(file, delimiter=';')
        writer.writerow(row)
    return


def main():
    # Input variables.

    # Region can be specified by setting the "city" variable to one of those:
    # almaty, astana, shymkent, abay-oblast, akmolinskaja-oblast, aktjubinskaja-oblast, almatinskaja-oblast
    # atyrauskaja-oblast, vostochno-kazahstanskaja-oblast, zhambylskaja-oblast, jetisyskaya-oblast,
    # zapadno-kazahstanskaja-oblast, karagandinskaja-oblast, kostanajskaja-oblast, kyzylordinskaja-oblast,
    # mangistauskaja-oblast, pavlodarskaja-oblast, severo-kazahstanskaja-oblast, juzhno-kazahstanskaja-oblast,
    # ulitayskay-oblast
    # For accessing a specific city/town checkout the url for the necessary string
    page = 1
    city = 'almaty'

    # Change headers too if needed
    headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                             '(KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36'}
    main_card_class = 'a-card a-storage-live'
    header_class = 'a-card__title'
    address_class = 'a-card__subtitle'
    price_class = 'a-card__price'
    owner_class = 'owners__label'
    date_class = 'card-stats__item'
    month_string = r'янв.|фев.|мар.|апр.|май.|июн.|июл.|авг.|сен.|окт.|ноя.|дек.'
    month_mapping = {'янв.':'January', 'фев.':'February', 'мар.':'March', 'апр.':'April', 'май.':'May',
                     'июн.':'June', 'июл.':'July', 'авг.':'August','сен.':'September', 'окт.':'October',
                     'ноя.':'November', 'дек.':'December'}
    path_to_write = fr'D:\venvs\krisha-scrapper\raw_data\rent-apartments-{city}.csv'


    while True:

        url = f'https://krisha.kz/arenda/kvartiry/{city}/?das[_sys.hasphoto]=1&das' \
              f'[floor_not_first]=1&rent-period-switch=%2Farenda%2Fkvartiry&page={page}'
        app_cards = get_request(url=url, headers=headers, card_class=main_card_class)

        if len(app_cards):
            for app in app_cards:
                # The information of each post formates a new list called row
                row = []

                header = parse_header(header_class=header_class, app_card=app)
                for item in header:
                    row.append(item)

                adress = parse_address(address_class=address_class, app_card=app)
                for item in adress:
                    row.append(item)

                price = parse_price(price_class=price_class, app_card=app)
                row.append(price)

                owner = parse_owner(owner_class=owner_class, app_card=app)
                row.append(owner)

                date = parse_date(date_class=date_class, app_card=app, month_string=month_string)
                # The original date format is getting changed in order to ease up further processing
                for k, v in month_mapping.items():
                    date = date.replace(k, v)
                row.append(date)

                url = parse_url(app_card=app)
                row.append(url)

                # After list row recevied all the necessary data, its written to a csv file
                writer(path_to_write, row=row)

            print(f'Page {page} has been succesfully processed!')
            # As soon as the app has finished processing a page, page number increases by one
            page += 1

            # In order to bypass pottential blocking, sleep time of 5 to 10 seconds is used
            # This will make the process appear human-like. Shorter sleep time is optional
            time.sleep(random.randint(5,10))
        else:
            print(f'---------------------------------\n'
                  f'Reached the final page, data is ready!')
            # The part below is optional. It writes scraped data into an ADLS Gen2 container
            # Connection string and container name are to be provided
            # Comment out the code till "break" in case it is not needed
            print(f'---------------------------------\n'
                  f'Writing data to the ADLS Gen2 Container')
            load_dotenv()

            # Create .env file in the working directory and add a connenction string variable
            connection_string = os.environ['STORAGE_CONNECTION_STRING']
            container_name = 'krisha-almaty'
            write_to_ADLS(connection_string, container_name, local_path=path_to_write, city=city)
            if os.path.exists(path_to_write):
                print(f'--------------------------------\n'
                      f'Data has been successfully written to the ADLS Gen2 container')
                os.remove(path_to_write)
                print(f'--------------------------------\n'
                      f'File {path_to_write} has been deleted from local storage')
            else:
                print(f'File {path_to_write} does not exist')
            break

if __name__ == '__main__':
    main()
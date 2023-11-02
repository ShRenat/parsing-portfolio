import json
import logging
import requests
import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/118.0',
    'Accept': '*/*',
    'Accept-Language': 'ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3',
    # 'Accept-Encoding': 'gzip, deflate, br',
    'Referer': 'https://www.sberbank.ru/ru/quotes/currencies?tab=kurs&currency=EUR&package=ERNP-4',
    'Connection': 'keep-alive',
    # 'Cookie': 'JSESSIONID=Uf2ipY0rYRjHF8e6GwRoXcafmHP1ALGYYOE3DZ8S.gateway-689674c69b-hff75; TS019fab19=013ade2899c7055ed782f2ebd35dc2bae3402f5010ef703e1d97a048c22b5042b722f773155a98d590d0904e890d92dc342bdc8382cac64be8115a5790a04fb84818dcbd9b8ab89f3ab7504a3873b05f66b6a1f80ff24cd5d1395039f27161e01e939b8c6584c8f988cbca11a49e9a2b8fe8c12e8331ab982b5435e566f7e4a17fe940c3f7ca1ace6155835c74cd71288ef996b7e3201babc1d1da2f0d15b46cb921ec2215d3a9395b21db9c9f895224a9e7a109652d3c4a284d969881e812d80cf77d1f47; _sa=SA1.4eda441e-da7c-4e06-b055-b000055b4509.1694766714; sb-pid=gYEXQbQRAEVDXKC6yA16jhqyAAABipf30TFebYWRfqJhVxtARe821hnGP_aOn1NYGznqfsvWzhSj-g; sbrf.pers_sign=1; g_uid=xVjFouKlxSoEUzwjhsRFBPxQVeYtQKLSKxHAXtJFZU0G95wyW3lqZPWzOMnu/HAPtwHt6/UJcQ1V9zJ2P51+vFqox9icyLZHXRviAg==; BBXSRF=337d4d6f-767c-42bd-9773-9fb8b854c2d8; JSESSIONID=8E1zTKsmC1sBnZxMdTp3xWrvf45oHs4T-yVyms5h.portalserver-live-6fbbc55bf8-tpd2t; 8f7f8c377e0f2a8171e27c3bfec6a87a=4ba91426b9f718a6f88c2e3bcf7a78f3; TS011f2bf6=013ade28992d1db37d11e61d5663cc46e6d315a9eec77e35e08c1b87a8471ddca3411dcd9c10a90110edbaf428c8d383c2a9172ceaeb88fb3300348e0798d57cac4b7518cea3397a6e6c3a26bac576e29555d4c277ce01a163b3d9d0f46d2539ce770f8d2a79a8b98b70998689ea66d6b215ed83c351e6bd68d7c84542406b9f71ae6e4730ac484c0e5497403ecb2eac8d01182f6dd34829b57a69e54ce8dbe9f5ee513d3c7a35ad30f9f121d50dae9c228b591bf4079bd81434653bbd110eb254b1d858e821ff556bd9e189f01ee38a542d66eb52ee22f91c8f803a6bfe8ff9a564e3bbade9498ec247eb94bddebecdeae252e38b; TS1583a86a027=08fbdc5594ab20006c0395beda0e9d1bf203237456ee9a78f9b1de8235ef27328dcd28ce34ed13c508d781b28d113000dde2b7b9d6a36a648ec93c177c90625a90f450fd917a1b6dceb458ef97225d823c4f9b4cd9e3203035ac302e873ec9f0; TS00000000076=08fbdc5594ab2800c935ff5a1f3d5e43b1b754bce511a24c56138e0799bf8e2aa46cee1518b8efa7ca9e74724b806aa60841c06c7609d0006bb31cf7c073a5dfdfbe78c47c2df89f18357de77f21fd835e88eca67af246faf119b67a2a92382b5743172c62ff0b8c85e20a0f9d784a6f6ad80756bf7bbf3398e3fcbbee798bbff3faa40b78933867bf1d1f330f1dfad1bfcb256cd6202f356b89b452d5c7dbca2bb9a09acfe35df776cd08e4e3310150e38ef087d8e093fb8acfd7ef69f5f53a1cebda8723f612db4f7db1d9bfc2afbab38f08732ef2ec5cebbc2eb66776c3ccdd47d2045994ae2b87f3dbf33fc0c26bef219221645e7da8ff6344a26f30baa751a4a75e68db7b77; TSPD_101_DID=08fbdc5594ab2800c935ff5a1f3d5e43b1b754bce511a24c56138e0799bf8e2aa46cee1518b8efa7ca9e74724b806aa60841c06c7606380057f643fb1426ce2d100dcaae93f781904c5e611b93caab3f43e3eb5b74b6066a64a6475b756e5e5263b03d25dc19c08503af4dca1ba169d4; TSPD_101=08fbdc5594ab2800aa82f53bf8ac690331987aba7b37a77e3c59542254b18835ef32a85e385f296bdd8d2c854991435a0843c399f4051800dbbf7d08c5f2a6689a73bc033e85f060ed8adf5971dd1a02; abc4e19df5455fc72f51575e0d5bd928=bc7c387de68a1e671e3f0e228cd61def; f5avraaaaaaaaaaaaaaaa_session_=FBKJOCFIFNCGDIFFIPEKKKLIDPEHLJLBIECFFOKDNMHIFEPKMKBLCPNNJNMNKNLMKGDDBNGLLJIAEOHHKOAABOLCEFDELJGAEBHHEEMEGNBCNMKLDANHPDNBLNOANIMH; TS89e18e75077=08fbdc5594ab2800725b4efdd40c75153f715e0c66ef281383da8dfae3f9d034c5d9b416159fbf8c3ca8afc2d7c6e64d0845ade2bf172000fcc4370d2ef60bbe70d0e118b68702814c8ca70899375f29e83333f9a14f7ae4; fd4d781bd0c8fb3a051334d9d1fc792d=97e719473592d7fcb559eef8feffa93e; cf44ad4bdad05ee181f953b4c4e5e921=5b9b6b491bf9c08c3a1399b8f858225d; TS011965e5=013ade28998e977c03b1a306a67a1adcf6bdf876a3217c0368d9ca182438aaecd70ebc24d9837c0781009499f83e6612801a711981; TScb26698b027=08fbdc5594ab2000cddb7c37acf5ac87422cc1b2b6553e1c8d7ed881ba09248ec407c721bc4baefd08ac4f5cb81130005893d6c05ab9244ac9396ec6ac3faf44c25eee28dc040468bacdf7065e562c876c6441890fc6c3bc77dbae94a1282770; TS01929ac3=013ade289915fe0924dc64334cbeef4a78c6d9f86cb584c53caf4bec83ea1fb1a4ea3701bac2f9a8d0668bbd3f667aef47ecf3e7de8c1f8efa5fe51cf386ded9f71538209cd5d3e29351e1905aeef9975d111586bc2e7a24e60978fd9e8263009b4609a9da2ad059e0a84332198d9b44f9304af93e4081a2af4c162481d2f90bb2cb9ef656e40566b5736a78f2c6e6a2c3bb4cff849ba0763f70dfd82ac3696b9ab0c4e1dfad25ff57955475e1646a7df831be9faa; TS57444f94077=08fbdc5594ab2800f9787347d4561355c57be2467c44001380a38e552c625b6c4982b98d6ee049da69ff8384bfe4e428089c3ab6401720009cc4b71c62058a4dc03fcf9574426ae0f59d3616a66bff522d3e7bcc9e7aa8fb; TS019a42f2=0156c5c860813e6e654ab9b6041d0766dce3dc685536841b735f6381309c4ca0d0ab81927b3316e3d823912832e48fefe9e97d7f5b9ee51757f00bf9079bf1d19331bbe96364608743dae35a20c1d1d18f0deaef55018aa9d8ebeb38bb3b08378b7ed523c6f6d5df1b32daef7286771484da36740acd89cdf792619b3b539906649c833d6fffacb123e8d36230c15be990578f82ee; sbrf.region_id=16; x-session-id=e4ca3f9b-d64a-582a-715d-ef81e9ea55cf; sbrf.region_manual=true; sber.pers_notice=1; sbrf.region_set=true',
    'Sec-Fetch-Dest': 'script',
    'Sec-Fetch-Mode': 'no-cors',
    'Sec-Fetch-Site': 'same-origin',
    'If-Modified-Since': 'Fri, 29 Sep 2023 13:39:21 GMT',
    'If-None-Match': 'W/28706-18ae12a4f28',
    # Requests doesn't support trailers
    # 'TE': 'trailers',
}

cookies = {
    'JSESSIONID': 'Uf2ipY0rYRjHF8e6GwRoXcafmHP1ALGYYOE3DZ8S.gateway-689674c69b-hff75',
    'TS019fab19': '013ade2899c7055ed782f2ebd35dc2bae3402f5010ef703e1d97a048c22b5042b722f773155a98d590d0904e890d92dc342bdc8382cac64be8115a5790a04fb84818dcbd9b8ab89f3ab7504a3873b05f66b6a1f80ff24cd5d1395039f27161e01e939b8c6584c8f988cbca11a49e9a2b8fe8c12e8331ab982b5435e566f7e4a17fe940c3f7ca1ace6155835c74cd71288ef996b7e3201babc1d1da2f0d15b46cb921ec2215d3a9395b21db9c9f895224a9e7a109652d3c4a284d969881e812d80cf77d1f47',
    '_sa': 'SA1.4eda441e-da7c-4e06-b055-b000055b4509.1694766714',
    'sb-pid': 'gYEXQbQRAEVDXKC6yA16jhqyAAABipf30TFebYWRfqJhVxtARe821hnGP_aOn1NYGznqfsvWzhSj-g',
    'sbrf.pers_sign': '1',
    'g_uid': 'xVjFouKlxSoEUzwjhsRFBPxQVeYtQKLSKxHAXtJFZU0G95wyW3lqZPWzOMnu/HAPtwHt6/UJcQ1V9zJ2P51+vFqox9icyLZHXRviAg==',
    'BBXSRF': '337d4d6f-767c-42bd-9773-9fb8b854c2d8',
    'JSESSIONID': '8E1zTKsmC1sBnZxMdTp3xWrvf45oHs4T-yVyms5h.portalserver-live-6fbbc55bf8-tpd2t',
    '8f7f8c377e0f2a8171e27c3bfec6a87a': '4ba91426b9f718a6f88c2e3bcf7a78f3',
    'TS011f2bf6': '013ade28992d1db37d11e61d5663cc46e6d315a9eec77e35e08c1b87a8471ddca3411dcd9c10a90110edbaf428c8d383c2a9172ceaeb88fb3300348e0798d57cac4b7518cea3397a6e6c3a26bac576e29555d4c277ce01a163b3d9d0f46d2539ce770f8d2a79a8b98b70998689ea66d6b215ed83c351e6bd68d7c84542406b9f71ae6e4730ac484c0e5497403ecb2eac8d01182f6dd34829b57a69e54ce8dbe9f5ee513d3c7a35ad30f9f121d50dae9c228b591bf4079bd81434653bbd110eb254b1d858e821ff556bd9e189f01ee38a542d66eb52ee22f91c8f803a6bfe8ff9a564e3bbade9498ec247eb94bddebecdeae252e38b',
    'TS1583a86a027': '08fbdc5594ab20006c0395beda0e9d1bf203237456ee9a78f9b1de8235ef27328dcd28ce34ed13c508d781b28d113000dde2b7b9d6a36a648ec93c177c90625a90f450fd917a1b6dceb458ef97225d823c4f9b4cd9e3203035ac302e873ec9f0',
    'TS00000000076': '08fbdc5594ab2800c935ff5a1f3d5e43b1b754bce511a24c56138e0799bf8e2aa46cee1518b8efa7ca9e74724b806aa60841c06c7609d0006bb31cf7c073a5dfdfbe78c47c2df89f18357de77f21fd835e88eca67af246faf119b67a2a92382b5743172c62ff0b8c85e20a0f9d784a6f6ad80756bf7bbf3398e3fcbbee798bbff3faa40b78933867bf1d1f330f1dfad1bfcb256cd6202f356b89b452d5c7dbca2bb9a09acfe35df776cd08e4e3310150e38ef087d8e093fb8acfd7ef69f5f53a1cebda8723f612db4f7db1d9bfc2afbab38f08732ef2ec5cebbc2eb66776c3ccdd47d2045994ae2b87f3dbf33fc0c26bef219221645e7da8ff6344a26f30baa751a4a75e68db7b77',
    'TSPD_101_DID': '08fbdc5594ab2800c935ff5a1f3d5e43b1b754bce511a24c56138e0799bf8e2aa46cee1518b8efa7ca9e74724b806aa60841c06c7606380057f643fb1426ce2d100dcaae93f781904c5e611b93caab3f43e3eb5b74b6066a64a6475b756e5e5263b03d25dc19c08503af4dca1ba169d4',
    'TSPD_101': '08fbdc5594ab2800aa82f53bf8ac690331987aba7b37a77e3c59542254b18835ef32a85e385f296bdd8d2c854991435a0843c399f4051800dbbf7d08c5f2a6689a73bc033e85f060ed8adf5971dd1a02',
    'abc4e19df5455fc72f51575e0d5bd928': 'bc7c387de68a1e671e3f0e228cd61def',
    'f5avraaaaaaaaaaaaaaaa_session_': 'FBKJOCFIFNCGDIFFIPEKKKLIDPEHLJLBIECFFOKDNMHIFEPKMKBLCPNNJNMNKNLMKGDDBNGLLJIAEOHHKOAABOLCEFDELJGAEBHHEEMEGNBCNMKLDANHPDNBLNOANIMH',
    'TS89e18e75077': '08fbdc5594ab2800725b4efdd40c75153f715e0c66ef281383da8dfae3f9d034c5d9b416159fbf8c3ca8afc2d7c6e64d0845ade2bf172000fcc4370d2ef60bbe70d0e118b68702814c8ca70899375f29e83333f9a14f7ae4',
    'fd4d781bd0c8fb3a051334d9d1fc792d': '97e719473592d7fcb559eef8feffa93e',
    'cf44ad4bdad05ee181f953b4c4e5e921': '5b9b6b491bf9c08c3a1399b8f858225d',
    'TS011965e5': '013ade28998e977c03b1a306a67a1adcf6bdf876a3217c0368d9ca182438aaecd70ebc24d9837c0781009499f83e6612801a711981',
    'TScb26698b027': '08fbdc5594ab2000cddb7c37acf5ac87422cc1b2b6553e1c8d7ed881ba09248ec407c721bc4baefd08ac4f5cb81130005893d6c05ab9244ac9396ec6ac3faf44c25eee28dc040468bacdf7065e562c876c6441890fc6c3bc77dbae94a1282770',
    'TS01929ac3': '013ade289915fe0924dc64334cbeef4a78c6d9f86cb584c53caf4bec83ea1fb1a4ea3701bac2f9a8d0668bbd3f667aef47ecf3e7de8c1f8efa5fe51cf386ded9f71538209cd5d3e29351e1905aeef9975d111586bc2e7a24e60978fd9e8263009b4609a9da2ad059e0a84332198d9b44f9304af93e4081a2af4c162481d2f90bb2cb9ef656e40566b5736a78f2c6e6a2c3bb4cff849ba0763f70dfd82ac3696b9ab0c4e1dfad25ff57955475e1646a7df831be9faa',
    'TS57444f94077': '08fbdc5594ab2800f9787347d4561355c57be2467c44001380a38e552c625b6c4982b98d6ee049da69ff8384bfe4e428089c3ab6401720009cc4b71c62058a4dc03fcf9574426ae0f59d3616a66bff522d3e7bcc9e7aa8fb',
    'TS019a42f2': '0156c5c860813e6e654ab9b6041d0766dce3dc685536841b735f6381309c4ca0d0ab81927b3316e3d823912832e48fefe9e97d7f5b9ee51757f00bf9079bf1d19331bbe96364608743dae35a20c1d1d18f0deaef55018aa9d8ebeb38bb3b08378b7ed523c6f6d5df1b32daef7286771484da36740acd89cdf792619b3b539906649c833d6fffacb123e8d36230c15be990578f82ee',
    'sbrf.region_id': '16',
    'x-session-id': 'e4ca3f9b-d64a-582a-715d-ef81e9ea55cf',
    'sbrf.region_manual': 'true',
    'sber.pers_notice': '1',
    'sbrf.region_set': 'true',
}


def get_json_data(url: str) -> dict:
    response = requests.get(url, headers=headers, cookies=cookies, verify=False)
    json_data = json.loads(response.text)

    return json_data


def process_json_data(json_data: dict) -> list:
    # Для отправки в гугл-таблицы нужно было сохранять список в списке
    currency_rate_list = []

    # Get TJS
    tjs_data = json_data.get('TJS', {}).get('rateList', [])[0]

    if tjs_data:
        tjs_rate_sell = tjs_data.get('rateSell', '')
        tjs_rate_buy = tjs_data.get('rateBuy', '')

        currency_rate_list.append(['TJS', tjs_rate_sell, tjs_rate_buy])
    else:
        logging.info('Данные для TJS не найдены.')

    # Get KZT
    kzt_data = json_data.get('KZT', {}).get('rateList', [])[0]

    if kzt_data:
        kzt_rate_sell = kzt_data.get('rateSell', '')
        kzt_rate_buy = kzt_data.get('rateBuy', '')

        currency_rate_list.append(['KZT', kzt_rate_sell, kzt_rate_buy])
    else:
        logging.info('Данные для KZT не найдены.')

    # Get AED
    aed_data = json_data.get('AED', {}).get('rateList', [])[0]

    if aed_data:
        aed_rate_sell = aed_data.get('rateSell', '')
        aed_rate_buy = aed_data.get('rateBuy', '')

        currency_rate_list.append(['AED', aed_rate_sell, aed_rate_buy])
    else:
        logging.info('Данные для AED не найдены.')

    # Get KGS
    kgs_data = json_data.get('KGS', {}).get('rateList', [])[0]

    if kgs_data:
        kgs_rate_sell = kgs_data.get('rateSell', '')
        kgs_rate_buy = kgs_data.get('rateBuy', '')

        currency_rate_list.append(['KGS', kgs_rate_sell, kgs_rate_buy])
    else:
        logging.info('Данные для KGS не найдены.')

    # Get BYN
    byn_data = json_data.get('BYN', {}).get('rateList', [])[0]

    if byn_data:
        byn_rate_sell = byn_data.get('rateSell', '')
        byn_rate_buy = byn_data.get('rateBuy', '')

        currency_rate_list.append(['BYN', byn_rate_sell, byn_rate_buy])
    else:
        logging.info('Данные для BYN не найдены.')

    # Get USD
    usd_data = json_data.get('USD', {}).get('rateList', [])[0]

    if usd_data:
        usd_rate_sell = usd_data.get('rateSell', '')
        usd_rate_buy = usd_data.get('rateBuy', '')

        currency_rate_list.append(['USD', usd_rate_sell, usd_rate_buy])
    else:
        logging.info('Данные для USD не найдены.')

    # Get VND
    vnd_data = json_data.get('VND', {}).get('rateList', [])[0]

    if vnd_data:
        vnd_rate_sell = vnd_data.get('rateSell', '')
        vnd_rate_buy = vnd_data.get('rateBuy', '')

        currency_rate_list.append(['VND', vnd_rate_sell, vnd_rate_buy])
    else:
        logging.info('Данные для VND не найдены.')

    # Get EUR
    eur_data = json_data.get('EUR', {}).get('rateList', [])[0]

    if eur_data:
        eur_rate_sell = eur_data.get('rateSell', '')
        eur_rate_buy = eur_data.get('rateBuy', '')

        currency_rate_list.append(['EUR', eur_rate_sell, eur_rate_buy])
    else:
        logging.info('Данные для EUR не найдены.')

    # Get AMD
    amd_data = json_data.get('AMD', {}).get('rateList', [])[0]

    if amd_data:
        amd_rate_sell = amd_data.get('rateSell', '')
        amd_rate_buy = amd_data.get('rateBuy', '')

        currency_rate_list.append(['AMD', amd_rate_sell, amd_rate_buy])
    else:
        logging.info('Данные для AMD не найдены.')

    return currency_rate_list


def add_to_google_sheet(data_to_send: list):
    import gspread
    from oauth2client.service_account import ServiceAccountCredentials

    credentials_file = 'model-sphere-322410-ae646cd5e227.json'
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    table_url = 'https://docs.google.com/spreadsheets/d/1Sh4Udqd3uoi8OOQhX7aeFVP7lysF8N11-g_IuE-fQE4/edit#gid=0'

    credentials = ServiceAccountCredentials.from_json_keyfile_name(credentials_file, scope)
    gc = gspread.authorize(credentials)

    worksheet = gc.open_by_url(table_url).sheet1

    for row in data_to_send:
        worksheet.append_rows([row])

    logging.info(f'Send {data_to_send} to google sheet.')


def save_currency_rate_to_csv(data_to_save: list, file_name: str):
    df = pd.DataFrame(data_to_save)
    df.to_csv(file_name, sep=';', header=False, index=False, mode='a')


def process_json_data_refactoring(json_data: dict) -> list:
    # Для отправки в гугл-таблицы нужно было сохранять список в списке
    currency_rate_list = []

    currencies = json_data.keys()

    for currency in currencies:
        currency_data = json_data[currency].get('rateList', [])[0]

        if currency_data:
            rate_sell = currency_data.get('rateSell', '')
            rate_buy = currency_data.get('rateBuy', '')

            currency_rate_list.append([currency, rate_sell, rate_buy])
        else:
            logging.info(f'Данные для {currency} не найдены.')

    return currency_rate_list


def main():
    url = 'https://www.sberbank.ru/proxy/services/rates/public/v2/actual?rateType=ERNP-4&isoCodes[]=KZT&isoCodes[' \
          ']=USD&isoCodes[]=BYN&isoCodes[]=EUR&isoCodes[]=AED&isoCodes[]=AMD&isoCodes[]=KGS&isoCodes[]=TJS&isoCodes[' \
          ']=VND&regionId=042'

    file_name = 'currency_rate.csv'

    raw_data = get_json_data(url)
    currency_data2 = process_json_data(raw_data)
    print(currency_data2)
    currency_data = process_json_data_refactoring(raw_data)

    for row in currency_data:
        save_currency_rate_to_csv([row], file_name)

    # add_to_google_sheet(currency_data)


if __name__ == '__main__':
    main()

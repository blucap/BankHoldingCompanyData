#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Dec 30 16:45:43 2018

@author: martien
# https://linuxhint.com/selenium-web-automation-python/
"""
import pandas as pd
import os
# from urllib.request import urlopen
from bs4 import BeautifulSoup
from selenium import webdriver
# from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import Select
from  more_itertools import unique_everseen
os.chdir('/home/../../../')

#%%
def soep2data(soup):
    table_body = soup.find('table', attrs={'class':'pubtables'})
    try:
        print(table_body)
        rows = table_body.find_all('tr')
        data = []
        for row in rows:
            cols = row.find_all('td')
            cols = [ele.text.strip() for ele in cols]
            data.append([ele for ele in cols if ele])
    except Exception:
        pass
    return(data)

def soep_form(url, da_form):
    driver = webdriver.Firefox(executable_path=r'/usr/local/bin/geckodriver')
    driver.implicitly_wait(30)
    driver.get(url)
    python_button = driver.find_element_by_id('search_by_Report')
    python_button.click()
    select = Select(driver.find_element_by_id('SelectedReportForm'))
    #select.select_by_value('187')
    select.select_by_visible_text(da_form)
    python_button = driver.find_element_by_id('Search')
    python_button.click()
    data = soep2data(BeautifulSoup(driver.page_source, 'lxml'))
    # driver.close()
    df = pd.DataFrame(data,columns=['item','description','confidential']).dropna(how = 'all')
    df = df.loc[pd.isnull(df.confidential), ['item', 'description']].set_index('item')
    driver.close()
    return(df)

def soep_series(url, da_series, *args):
    driver = webdriver.Firefox(executable_path=r'/usr/local/bin/geckodriver')
    driver.implicitly_wait(30)
    driver.get(url)
    python_text = driver.find_element_by_id('Keyword')
    python_text.send_keys(da_series)
    select = driver.find_element_by_id('search_by_Series')
    select.click()
    python_button = driver.find_element_by_id('Search')
    python_button.click()
    if da_series=='TEXT':
       da_series = [x for x in args][0]
       print(da_series)
       driver.implicitly_wait(15)
       select = Select(driver.find_element_by_id('FinalSeries'))
       select.select_by_visible_text(da_series)
       python_button = driver.find_element_by_id('submit')
       python_button.click()
    data = soep2data(BeautifulSoup(driver.page_source, 'lxml'))
    driver.close()
    df = pd.DataFrame(data,columns=['item','start','end', 'name', 'forms']).dropna(how = 'all')
    df = df[['item', 'start', 'end', 'name']].set_index('item')
    return(df)


def zoek1(x):
    return(dfr.loc[str(x).upper(), 'name'])


def zoek2(x):
    try:
        t = dff.loc[str(x).upper(), 'description']
    except Exception:
        t = "empty"
    return(t)



#%%
url = 'https://www.federalreserve.gov/apps/mdrm/data-dictionary'

dfr = soep_series(url, "RSSD")
dft = soep_series(url, "TEXT", "All")
df  = pd.concat([dfr, dft], sort="False")
dff = soep_form(url, 'FR Y-9C')

dff.to_csv('bhc_vars_mdrm_FRY9C_2020.csv')
df.to_csv('bhc_vars_mdrm_rssd_2020.csv')




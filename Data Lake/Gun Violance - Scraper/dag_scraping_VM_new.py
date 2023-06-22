#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Mar 31 10:48:28 2023
@author: paulina
"""
from selenium import webdriver
#from undetected_chromedriver import Chrome, ChromeOptions
from bs4 import BeautifulSoup
import pandas as pd
import time
import ssl
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import requests
import io
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import  BashOperator
from airflow.operators.postgres_operator import PostgresOperator
import datetime
import os
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.hooks.postgres_hook import PostgresHook
from webdriver_manager.chrome import ChromeDriverManager
import time
import datetime

def scraping():
    
    """ Webscraping function responsible for downloading the data from www.gunviolancearchieve.org
        The function uses Selenium and Headless Chrome driver. The function needs to wait until the page is loaded and
        download button is available after wards the data contained in the CSV file can be downloaded.
        At the end of the function, the postgresshook is called. The data is saved into AWS RDS. 
        The credentials are to be specified in the connection panel in the GUI of Apache Airflow.
        Due to the change of the webpage at the end of May, the scraper was adjusted and summarized some columns into the old structure.
    """
    my_ua = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36"  ## gepüfft auf https://www.whatsmyua.info/
    link = "https://www.gunviolencearchive.org/query/1e1fc646-a270-420e-8b41-3f497794831f/export-csv" 
    opts = Options()
    CHROMEDRIVER_PATH = '/usr/local/bin/chromedriver'
    opts.add_argument("user-agent=" + my_ua)
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--start-maximized")
    opts.add_argument("--headless")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-extensions")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("disable-gpu")
    print("Open driver")
    driver = webdriver.Chrome(ChromeDriverManager().install(),options=opts) #executable_path=CHROMEDRIVER_PATH,
    driver.get(link)
    print("Wait for button")
    cookie_button = WebDriverWait(driver, 30).until(
        EC.element_to_be_clickable((By.XPATH, '//*[@class="button big"]')))  
    print("transform to bs4")
    page_source = driver.page_source
    soup = BeautifulSoup(page_source, 'lxml')
    print("find csv")
    button = soup.find("a", class_="button big", href=True)
    print("get csv")
    url="https://www.gunviolencearchive.org"+button["href"]
    #bypass proxy
    os.environ['HTTP_PROXY'] ="*"
    session_obj=requests.Session()
    time.sleep(20)
    response = session_obj.get(url, headers={"User-Agent": "Mozilla/5.0"})
    df = pd.read_csv(io.StringIO(response.text))
    today = datetime.date.today()
    yesterday=today-datetime.timedelta(days=1)
    df=df[df["Incident Date"]==yesterday.strftime("%B %d, %Y")]
    df["# Injured"]=df["# Victims Injured"]+df["# Subjects-Suspects Injured"]
    df["# Killed"]=df["# Victims Killed"]+df["# Subjects-Suspects Killed"]
    
    df=df.rename(columns={'Incident ID':"Incident_ID","# Injured":"Injured", "# Killed":"Killed", "City Or County":"City_Or_County", 'Incident Date':'Incident_Date'})
    df=df[["Incident_ID","Incident_Date","State","City_Or_County","Address","Killed","Injured","Operations"]]
    print(df)
    
    rds_hook = PostgresHook(postgres_conn_id='postgres_aws_prod')
    rds_hook.insert_rows('gun_violance', df.values)


    
dag =DAG(
    
    'Scraping_gun_new',
    catchup = False, 
    schedule_interval = '0 4 * * *',
    start_date=datetime.datetime(2023, 5, 23,4,00)
) 
  
   
    

scraping = PythonOperator(
    task_id="scraping",
    python_callable=scraping,
    dag=dag
)

create_table = PostgresOperator(
  
    task_id="create_table",
    dag=dag,
    postgres_conn_id='postgres_aws_prod',
    sql='''
            CREATE TABLE IF NOT EXISTS gun_violance (Incident_ID integer,Incident_Date date, State VARCHAR(30), City_Or_County VARCHAR(50),Address VARCHAR(100), Killed integer, Injured integer, Operations Varchar(20));
        '''
)
create_table  >> scraping

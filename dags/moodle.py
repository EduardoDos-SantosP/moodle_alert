import asyncio
import csv
from collections import namedtuple
from datetime import datetime, timedelta
from itertools import groupby

import pandas
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

import os
import time
from datetime import datetime
from typing import List, Tuple, Dict, Any

from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.core.utils import ChromeType

import telegram
from airflow.hooks.base import BaseHook


class TelegramBot(BaseHook):
    def __init__(self):
        super().__init__()
        self.token: str = os.getenv('BOT_TOKEN')
        self.chat: str = os.getenv('CHAT_ID')

    def send_message(self, message: str):
        bot = telegram.Bot(token=self.token)
        asyncio.run(bot.send_message(chat_id=self.chat, text=message))


class Scrap(webdriver.Chrome):
    def __init__(self):
        service = Service(
            ChromeDriverManager(chrome_type=ChromeType.CHROMIUM).install()
        )
        options = webdriver.ChromeOptions()
        options.add_experimental_option('prefs', {
            'profile.managed_default_content_settings.images': 2
        })
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--headless')

        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option("useAutomationExtension", False)

        super(Scrap, self).__init__(service=service, options=options)

        self.waiter = WebDriverWait(self, 7)

        self.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

        self.implicitly_wait(5)
        self.set_script_timeout(30)

    def executar_scrap(self):
        url_site = 'https://presencial.ifgoiano.edu.br/login'
        self.get(url_site)
        print(f'Acessando ${self.title} em ${url_site}')

        param_input_nome = (By.ID, 'username')
        self.waiter.until(EC.element_to_be_clickable(param_input_nome))
        input_nome = self.find_element(*param_input_nome)
        time.sleep(2)
        input_nome.send_keys(os.getenv('MOODLE_LOGIN'))

        input_senha = self.find_element(By.ID, 'password')
        time.sleep(2)
        input_senha.send_keys(os.getenv('MOODLE_SENHA'))

        time.sleep(1)
        self.find_element(By.ID, 'loginbtn').click()

        print(self.title)
        self.waiter.until(EC.url_to_be('https://presencial.ifgoiano.edu.br/my/'))

        time.sleep(2)
        # paginacao = '[data-filtername=next30days]'
        paginacao = '[data-filtername=all]'
        self.waiter.until(EC.presence_of_element_located((By.CSS_SELECTOR, paginacao)))

        self.execute_script(f'''
            document.querySelector('{paginacao}').click()
        ''')
        time.sleep(3)

        tarefas: list[Dict] = self.execute_script(f'''
            return [...document.querySelectorAll('[data-region=event-list-content] li.media')].map(div => {{
                const a = div.querySelector('a')
                const tarefa = a.getAttribute('title')
                const data = div.parentElement.previousElementSibling.innerText
                const hora = div.querySelector('small.text-right').innerText
                const disciplina = a.lastElementChild.innerText
                const link = a.href
                
                return {{ tarefa, data, hora, disciplina, link }}
                return `${{tarefa}}: \n${{data}} ${{hora}}\nDisciplina: ${{disciplina}}\nLink do Moodle: ${{link}}`
            }})
        ''')

        tarefas_por_data: dict[str, list] = {}
        for data, tarefas_data in groupby(tarefas, lambda t: t['data']):
            tarefas_por_data[data] = list(tarefas_data)

        qtd_tarefas_por_data = list({data: len(tarefas_por_data[data])}
                                    for data in list(tarefas_por_data.keys()))

        pandas.DataFrame(qtd_tarefas_por_data)\
            .to_csv('tarefas_por_data.csv', index=False, quoting=csv.QUOTE_MINIMAL)

        mensagen = 'Atividades para serem feitas:\n\n' + '\n\n'.join(
            t['tarefa'] + ':\n' +
            t['data'] + ' ' + t['hora'] +
            ';\nDisciplina: ' + t['disciplina'] +
            ';\nLink do Moodle: ' + t['link'] for t in tarefas
        )
        bot = TelegramBot()
        bot.send_message(mensagen)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}
dag = DAG(
    'moodle',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False
)


def scrape_amazon_wishlist():
    try:
        scrap = Scrap()
        scrap.executar_scrap()
    except BaseException as e:
        print('ERRO CAPTURADO')
        print(str(e))
        raise e


def notify_telegram():
    # seu código de notificação via telegram aqui
    pass


start = EmptyOperator(task_id='start', dag=dag)

scrape_data = PythonOperator(
    task_id='scrape_data',
    python_callable=scrape_amazon_wishlist,
    dag=dag
)

notify = PythonOperator(
    task_id='notify_telegram',
    python_callable=notify_telegram,
    dag=dag
)

end = EmptyOperator(task_id='end', dag=dag)

start >> scrape_data >> notify >> end

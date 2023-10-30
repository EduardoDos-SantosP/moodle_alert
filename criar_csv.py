import pandas as pd
import csv


def criar_csv():
    dados = [
        {'Nome': 'João', 'Idade': 30, 'Cidade': 'São Paulo'},
        {'Nome': 'Maria', 'Idade': 25, 'Cidade': 'Rio de Janeiro'},
        {'Nome': 'Carlos', 'Idade': 35, 'Cidade': 'Belo Horizonte'}
    ]
    pd.DataFrame(dados).to_csv('teste.csv', index=False, quoting=csv.QUOTE_MINIMAL)


class Pessoa:
    def __init__(self, nome, idade, cidade):
        self.nome = nome
        self.idade = idade
        self.cidade = cidade


if __name__ == '__main__':
    criar_csv()

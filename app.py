# -*- coding: utf-8 -*-
"""
API Rest Mongo
"""

from operator import itemgetter
import os
from flask import Flask, jsonify
from pymongo import MongoClient
from pandas import read_csv as pd_read_csv

CONN_STR = "mongodb://bigidea:bigidea@cluster0-shard-00-00-dlg8g.mongodb.net:27017,\
                          cluster0-shard-00-01-dlg8g.mongodb.net:27017,\
                          cluster0-shard-00-02-dlg8g.mongodb.net:27017/test?ssl=true&replicaSet=\
                          Cluster0-shard-0&authSource=admin&retryWrites=true"

MONGO_CLIENT = MongoClient(CONN_STR)

APP = Flask(__name__)
APP.config['JSON_SORT_KEYS'] = False

URL_BASE = "http://dados.recife.pe.gov.br"
RESOURCE = "/dataset/93273993-d92c-4162-8c4a-66c930590c31/resource"

MESES = {1: "Janeiro", 2: "Fevereiro", 3: "Março", 4: "Abril", 5: "Maio",
         6: "Junho", 7: "Julho", 8: "Agosto", 9: "Setembro", 10: "Outubro",
         11: "Novembro", 12: "Dezembro"}

@APP.route("/database_names")
def database_names():
    """
    Return databases names.
    """

    return jsonify(MONGO_CLIENT.list_database_names())

def get_dataset(local=True):
    """
    Carrega dataset direto de um URL
    """

    if local:
        dataset = pd_read_csv("156cco2012-2018.csv")
    else:
        url_dataset = "{}{}/d9888fbe-ff70-42eb-b45b-971c467576a0/download/156cco2019.csv"\
        .format(URL_BASE, RESOURCE)

        dataset = pd_read_csv(url_dataset, sep=';')

    return dataset


def get_database(database_name: str):
    """
    Conecta na base de dados.
    """

    # selecionando a base
    database = MONGO_CLIENT[database_name]

    return database

def get_collection(collection_name: str):
    """
    Recupera a collection na base de dados.
    """

    # conectando na base de dados
    database = get_database("bigidea-dev")

    # recuperando a collection
    collection = database[collection_name]

    return collection

def get_data_year(year: int):
    """
    Retorna os dados da base de dados de acordo com o ano
    """

    # recuperando a collection
    collection = get_collection("emlurb")

    result_query = collection.find({"ANO_DEMANDA": year})

    return result_query

@APP.route("/v1/pie_plot", methods=["GET"])
def pie_plot():
    """
    Retorna os dados para a construção do grafico de circulo(pizza)
    """

    #resultQuery = get_data_year(year)

    #dataset = pd_dataframe(list(resultQuery))

    dataset = get_dataset()

    # removendo a coluna _id
    #dataset.drop(columns=["_id"], inplace=True)

    #groupby_result = dataset.groupby("MES_DEMANDA").size()
    groupby_result = dataset.groupby("ANO_DEMANDA").size()\
                     .sort_values(ascending=False)

    list_dict = list()
    for index, value in zip(groupby_result.index, groupby_result.values):
        list_dict.append({"ano": index, "chamados": int(value)})

    # ordenandos os dados por ano
    list_dict = sorted(list_dict, key=itemgetter("ano"))

    return jsonify(list_dict)

@APP.route("/v1/bar_plot", methods=["GET"])
def bar_plot():
    """
    Retorna os dados para a construção do grafico de colunas/barras.
    """

    dataset = get_dataset()

    groupby_result = dataset.groupby("MES_DEMANDA").size()\
                     .sort_values(ascending=False)

    list_dict = list()
    for index, value in zip(groupby_result.index, groupby_result.values):
        list_dict.append({"mes": index, "chamados": int(value)})

    # ordenandos os dados por ano
    list_dict = sorted(list_dict, key=itemgetter("mes"))

    for index, value in enumerate(list_dict):
        list_dict[index]["mes"] = MESES[index + 1]

    return jsonify(list_dict)

@APP.route("/v1/line_plot", methods=["GET"])
def line_plot():
    """
    Retorna os dados para a construção do grafico de linhas.
    """

    dataset = get_dataset()

    list_dict = list()
    for ano in set(dataset["ANO_DEMANDA"].values):
        values = dataset[dataset["ANO_DEMANDA"] == ano].groupby("MES_DEMANDA")\
                 .size().values.tolist()
        list_dict.append({"ano": int(ano), "qt": values})

    # ordenandos os dados por ano
    list_dict = sorted(list_dict, key=itemgetter("ano"))

    return jsonify(list_dict)


if __name__ == "__main__":
    PORT = int(os.environ.get("PORT", 5000))
    APP.run(host="0.0.0.0", port=PORT)

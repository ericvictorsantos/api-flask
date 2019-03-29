# -*- coding: utf-8 -*-
"""
API Rest
"""

from operator import itemgetter
import os
from flask import Flask, jsonify
from pandas import read_csv as pd_read_csv

APP = Flask(__name__)
APP.config['JSON_SORT_KEYS'] = False

MESES = {1: "Janeiro", 2: "Fevereiro", 3: "Março", 4: "Abril", 5: "Maio",
         6: "Junho", 7: "Julho", 8: "Agosto", 9: "Setembro", 10: "Outubro",
         11: "Novembro", 12: "Dezembro"}

def get_dataset():
    """
    Carrega dataset.
    """

    dataset = pd_read_csv("156cco2012-2018.csv")

    return dataset

@APP.route("/v1/pie_plot", methods=["GET"])
def pie_plot():
    """
    Retorna os dados para a construção do grafico de circulo(pizza).
    """

    dataset = get_dataset()

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

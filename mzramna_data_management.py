import csv
import json
import os
import logging
import mysql.connector
from datetime import datetime
from random import randint
import ntplib


class MYfileManager:

    def __init__(self, loggin_name="fileManager", log_file="filemanager.log"):
        self.logging = loggingSystem.create(loggin_name, filename=log_file)

    def saveFile(self, arrayToSave, arquivo):
        with open(arquivo, 'w+') as file:
            self.logging.debug("salvo dado no arquivo:" + str(arquivo))
            for elemento in arrayToSave:
                file.write(json.dumps(elemento) + "\n")
            file.flush()
            file.close()

    def readFile(self, arquivo):
        retorno = []
        with open(arquivo, 'r') as file:
            self.logging.debug("lido dado no arquivo:" + str(arquivo))
            linha = file.readline()
            while (linha != ""):
                retorno.append(json.loads(linha))
                linha = file.readline()
            file.close()
        return retorno

    def saveFileDictArray(self, arrayToSave, arquivo):
        titulos = arrayToSave[0].keys()
        print(titulos)
        csvWrite = csv.DictWriter(open(arquivo, mode='a+'), fieldnames=titulos, delimiter=";")
        self.logging.debug("salvo dado no arquivo:" + str(arquivo))
        if os.stat(arquivo).st_size == 0:
            csvWrite.writeheader()
        for i in range(0, len(arrayToSave)):
            self.saveFileDict(arrayToSave[i], arquivo)

    def saveFileDict(self, elemento: dict, arquivo):
        with open(arquivo, mode='a+') as file:
            self.logging.debug("salvo dado no arquivo:" + str(arquivo))
            titulos = elemento.keys()
            csvWrite = csv.DictWriter(file, fieldnames=titulos, delimiter=";")
            if os.stat(arquivo).st_size == 0:
                for titulo in elemento:
                    self.logging.debug(titulo)
                csvWrite.writeheader()
            csvWrite.writerow(elemento)
            file.flush()
            file.close()

    def readFileDictArray(self, arquivo):
        retorno = []
        with open(arquivo, mode='r') as file:
            self.logging.debug("lido dado no arquivo:" + str(arquivo))
            csvReader = csv.DictReader(file, delimiter=";")
            line = 0
            for row in csvReader:
                if line == 0:
                    line += 1
                else:
                    retorno.append(row)
                    line += 1
            self.logging.debug("total de linhas lido: " + str(line))
            file.flush()
            file.close()
        return retorno

    def readFileDict(self, arquivo, line):
        with open(arquivo, mode='r') as file:
            self.logging.debug("lido dado no arquivo:" + str(arquivo))
            csvReader = csv.DictReader(file, delimiter=";")
            for i, row in enumerate(csvReader):
                if i == line:
                    retorno = row
                    file.flush()
                    file.close()
                    return retorno


class loggingSystem:
    def create(name, filename='arquivo.log', format='%(name)s - %(levelname)s - %(message)s', level=logging.DEBUG):
        formatter = logging.Formatter(format)
        handler = logging.FileHandler(filename)
        handler.setFormatter(formatter)

        logger = logging.getLogger(name)
        logger.setLevel(level)
        logger.addHandler(handler)

        return logger


class MYmysql:

    def __init__(self, user, passwd, host, database, loggin_name="mysql", log_file="mysql.log"):
        self.logging = loggingSystem.create(loggin_name, filename=log_file)

        self.db_connection = mysql.connector.connect(
            host=host,
            user=user,
            passwd=passwd,
            database=database
        )
        self.cursor = self.db_connection.cursor(dictionary=True)

    def getDb(self):
        return self.db_connection

    def convertDictHeaders(self, dict={}, conversao=[],
                           advanced_debug=False):  # conversao e passado assim: nomeDesejado:nomeNoDict
        dictConsulta = {}
        if advanced_debug:
            self.logging.debug("colunas iniciais" + str(dict.keys()))
        if conversao != []:
            for elemento in conversao:
                dictConsulta[elemento[0]] = dict[elemento[1]]
        else:
            dictConsulta = dict
        if advanced_debug:
            self.logging.debug("colunas finais" + str(dictConsulta.keys()))
        return dictConsulta

    def convertHeaders(self, headers=[], conversao=[], advanced_debug=False):  #
        returnHeader = {}
        if advanced_debug:
            self.logging.debug("colunas iniciais" + str(headers))
        for elemento in conversao:
            for header in headers:
                if elemento[1] == header:
                    returnHeader[elemento[0]] = ""
        colunas = returnHeader.keys()
        if advanced_debug:
            self.logging.debug("colunas iniciais" + str(colunas))
        return colunas

    def getElement(self, tabelaBuscada, valorBuscado=[], colunaResultado=["*"], colunaBuscada=["id"], conversao=[],
                   advanced_debug=False):
        self.cursor = self.db_connection.cursor(dictionary=True)
        sql = "SELECT "
        for i in colunaResultado:
            if colunaResultado.index(i) > 0 and colunaResultado.index(i) < (len(colunaResultado) - 1):
                sql += ","
            sql += i
        sql += " FROM " + tabelaBuscada
        if valorBuscado != [] and len(colunaBuscada) == len(valorBuscado):
            sql += " WHERE "
            for i in len(colunaBuscada):
                if i > 0:
                    sql += " AND "
                sql += str(colunaBuscada[i]) + " = '" + str(valorBuscado[i]) + "'"

        if advanced_debug:
            self.logging.debug(sql)
            self.cursor.execute(sql)
            rows = self.cursor.fetchall()
            for row in rows:
                self.logging.debug(row)
            if len(rows) > 0:
                self.logging.debug("colunas antes da conversao: " + str(rows[0].keys()))
            else:
                self.logging.debug("vazio")
        newRows = []
        if conversao != []:
            for row in rows:
                newRows.append(self.convertDictHeaders(row, conversao))
            rows = newRows
        if advanced_debug:
            if len(rows) > 0:
                self.logging.debug("colunas depois da conversao: " + str(rows[0].keys()))

            for row in rows:
                self.logging.debug(row)
        self.db_connection.commit()
        self.cursor.close()
        return rows

    def addElement(self, dict, tabela, conversao=[], advanced_debug=False):
        self.logging.debug("dict = " + str(dict) + " tabela = " + str(tabela) + " conversao = " + str(conversao))
        self.cursor = self.db_connection.cursor(dictionary=True)
        cabecalhos = self.convertDictHeaders(dict, conversao)
        sql = "INSERT INTO " + tabela + " ("
        cabecalhos_keys = list(cabecalhos.keys())
        if advanced_debug:
            self.logging.debug(str(cabecalhos_keys))
        for cabecalho in cabecalhos_keys:
            if cabecalho != cabecalhos_keys[0]:
                sql += ","
            sql += cabecalho
        sql += ") VALUES ("
        for cabecalho in cabecalhos_keys:
            if cabecalho != cabecalhos_keys[0]:
                sql += ","
            sql += '"' + str(cabecalhos[cabecalho]) + '"'
        sql += ")"
        if advanced_debug:
            self.logging.debug(sql)
        self.cursor.execute(sql)
        self.db_connection.commit()
        id = self.cursor.execute("SELECT LAST_INSERT_ID();")
        self.cursor.close()

        return id

    def addMultipleElement(self, dictArray, tabela, conversao=[]):
        for dict in dictArray:
            self.addElement(dict, tabela, conversao)

    def deleteElement(self, tabela, parametros=[], searchRow=["id"]):
        sql = "DELETE FROM " + tabela
        if not (parametros == [] or len(parametros) != len(searchRow)):
            sql += " WHERE "
            for i in range(0, len(parametros)):
                sql += str(searchRow[i]) + " = '" + str(parametros[i]) + "' "
                if i < len(parametros) - 1:
                    sql += "AND "
        self.logging.warning(sql)
        self.cursor = self.db_connection.cursor(dictionary=True)
        self.cursor.execute(sql)
        self.db_connection.commit()
        self.cursor.close()

    def updateElement(self, tabela, searchParametros=[], searchRow=["id"], advanced_debug=False):
        sql = "DELETE FROM " + tabela + " WHERE "
        if searchParametros == [] or len(searchParametros) != len(searchRow):
            raise Exception
        for i in range(0, len(searchParametros)):
            sql += str(searchRow[i]) + " = '" + str(searchParametros[i]) + "' "
            if i < len(searchParametros):
                sql += "AND "
        if advanced_debug:
            self.logging.debug(sql)
        self.cursor = self.db_connection.cursor(dictionary=True)
        self.cursor.execute(sql)
        self.db_connection.commit()
        self.cursor.close()


class MYntp():
    ntp = ntplib.NTPClient()

    def currentTime(self):
        ntpAderesses = ["a.st1.ntp.br", "b.st1.ntp.br", "c.st1.ntp.br", "d.st1.ntp.br", "a.ntp.br", "b.ntp.br",
                        "c.ntp.br", "gps.ntp.br"]
        i = randint(0, len(ntpAderesses))
        try:
            time = self.ntp.request(ntpAderesses[i], version=3).orig_time
            return time
        except:
            return self.currentTime(self)

    def convertUnixToReadable(self, time="empty"):
        if time == "empty":
            time = self.currentTime(self)
        return datetime.utcfromtimestamp(time).strftime('%Y-%m-%d %H:%M:%S')

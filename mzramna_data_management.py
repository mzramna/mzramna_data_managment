import csv
import json
import logging
import os
from datetime import datetime
from random import randint

import gspread
import mysql.connector
import ntplib
from gspread.models import Cell
from oauth2client.service_account import ServiceAccountCredentials


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


class MYG_Sheets():
    def __init__(self, json_file):
        self.creds = ServiceAccountCredentials.from_json_keyfile_name(json_file,
                                                                      ["https://spreadsheets.google.com/feeds",
                                                                       "https://www.googleapis.com/auth/spreadsheets",
                                                                       "https://www.googleapis.com/auth/drive.file",
                                                                       "https://www.googleapis.com/auth/drive"])
        self.client = gspread.authorize(self.creds)

    def create_sheet(self, sheet_name, owner=None, public=False):
        if public:
            return self.client.create(sheet_name).id
        elif owner == None:
            return self.client.create(sheet_name).id
        else:
            id = self.client.create(sheet_name).id
            self.client.insert_permission(id, owner, perm_type="user", role="owner")
            return id

    def delete_sheet(self, sheet_id):
        self.client.del_spreadsheet(sheet_id)

    def add_reader_sheet(self, sheet_id, email):
        self.client.insert_permission(sheet_id, email, perm_type="user", role="reader")

    def add_writer_sheet(self, sheet_id, email):
        self.client.insert_permission(sheet_id, email, perm_type="user", role="writer")

    def retrive_data(self, sheet_id, page_number, import_range="all"):
        sheet = self.client.open_by_key(sheet_id)
        if import_range == "all":
            return sheet.get_worksheet(page_number).get_all_records()
        else:
            return sheet.get_worksheet(page_number).range(import_range)

    def update_data_range(self, sheet_id, page_number, list_of_row, list_of_col, list_of_values):
        sheet = self.client.open_by_key(sheet_id)
        try:
            if len(list_of_col) != len(list_of_row) != len(list_of_values):
                raise Exception("invalid amount of elemnts on lists")
            else:
                cells = []
                for i in range(0, list_of_values):
                    cells.append(Cell(row=list_of_row[i], col=list_of_col[i], value=list_of_values[i]))
                sheet.get_worksheet(page_number).update_cells(cells)
        except Exception as exp:
            print(exp.args)

    def update_data_cell(self, sheet_id, page_number, cell_cood, new_value):
        sheet = self.client.open_by_key(sheet_id)
        try:
            if isinstance(cell_cood, str):
                sheet.get_worksheet(page_number).update_acell(cell_cood, new_value)
            elif type(cell_cood) == type([]) and len(cell_cood) == 2:
                sheet.get_worksheet(page_number).update_cell(cell_cood[0], cell_cood[1], new_value)
            else:
                raise Exception('invalid input cell_cood')
        except Exception as error:
            print(error.args)

    def delete_data_cell(self, sheet_id, page_number, cell_cood):
        sheet = self.client.open_by_key(sheet_id)
        try:
            if isinstance(cell_cood, str):
                sheet.get_worksheet(page_number).update_acell(cell_cood, "")
            elif type(cell_cood) == type([]) and len(cell_cood) == 2:
                sheet.get_worksheet(page_number).update_cell(cell_cood[0], cell_cood[1], "")
            else:
                raise Exception('invalid input cell_cood')
        except Exception as error:
            print(error.args)

    def delete_data_row(self, sheet_id, page_number, row_id):
        sheet = self.client.open_by_key(sheet_id)
        sheet.get_worksheet(page_number).delete_row(row_id)

    def add_page(self, sheet_id, sheet_name, minimum_col=24, minimum_row=10):
        sheet = self.client.open_by_key(sheet_id)
        sheet.add_worksheet(title=sheet_name, rows=minimum_row, cols=minimum_col)

    def delete_page(self, sheet_id, page_number):
        sheet = self.client.open_by_key(sheet_id)
        sheet.del_worksheet(page_number)

import csv
import json
import logging
import os
import sys
import traceback
from datetime import datetime
from operator import itemgetter
from random import randint
from time import sleep

import gspread
import mysql.connector
import ntplib
from gspread.models import Cell
from oauth2client import client
from oauth2client.client import OAuth2WebServerFlow
from oauth2client.file import Storage
from oauth2client.service_account import ServiceAccountCredentials
from oauth2client.tools import run_flow


class MYcsv:
    def __init__(self, loggin_name="csvManager", log_file="./csvManager.log"):
        """
        classe para gerenciar arquivos csv
        :param loggin_name: nome do log que foi definido para a classe,altere apenas em caso seja necessário criar multiplas insstancias da função
        :param log_file: nome do arquivo de log que foi definido para a classe,altere apenas em caso seja necessário criar multiplas insstancias da função
        """
        self.logging = loggingSystem(loggin_name, arquivo=log_file)

    def saveFileDictArray(self, arrayToSave, arquivo, advanced_debug=False):
        """
        função para salvar arquivos de tipo csv
        :param arrayToSave: array de tipo dictionary para ser salvo no arquivo
        :param arquivo: nome do arquivo csv a ser acessado
        :param advanced_debug: ativa o sistema de logging se definido para True
        :return:
        """
        titulos = arrayToSave[0].keys()
        # print(titulos)
        csvWrite = csv.DictWriter(open(arquivo, mode='a+'), fieldnames=titulos, delimiter=";")
        if advanced_debug:
            self.logging.debug("salvo dado no arquivo:" + str(arquivo))
        if os.stat(arquivo).st_size == 0:
            csvWrite.writeheader()
        for i in range(0, len(arrayToSave)):
            self.saveFileDict(arrayToSave[i], arquivo)

    def saveFileDict(self, elemento: dict, arquivo, advanced_debug=False):
        """
        função para salvar arquivos de tipo csv
        :param elemento: elemento de tipo dictionary para ser salvo ao arquivo
        :param arquivo: nome do arquivo csv a ser acessado
        :param advanced_debug: ativa o sistema de logging se definido para True
        :return:
        """
        with open(arquivo, mode='a+') as file:
            if advanced_debug:
                self.logging.debug("salvo dado no arquivo:" + str(arquivo))
            titulos = elemento.keys()
            csvWrite = csv.DictWriter(file, fieldnames=titulos, delimiter=";")
            if os.stat(arquivo).st_size == 0:
                if advanced_debug:
                    for titulo in elemento:
                        self.logging.debug(titulo)
                csvWrite.writeheader()
            csvWrite.writerow(elemento)
            file.flush()
            file.close()

    def readFileDictArray(self, arquivo, advanced_debug=False):
        """
        função para ler arquivos de tipo csv
        :param arquivo: nome do arquivo csv a ser acessado
        :param advanced_debug: ativa o sistema de logging se definido para True
        :return: array de dict dentro do arquivo csv
        """
        retorno = []
        with open(arquivo, mode='r') as file:
            if advanced_debug:
                self.logging.debug("lido dado no arquivo:" + str(arquivo))
            csvReader = csv.DictReader(file, delimiter=";")
            line = 1
            for row in csvReader:
                retorno.append(row)
                line += 1
            self.logging.debug("total de linhas lido: " + str(line))
            file.flush()
            file.close()
        return retorno

    def readFileDict(self, arquivo, line, advanced_debug=False):
        """
        função para ler arquivos de tipo csv
        :param arquivo: nome do arquivo csv a ser acessado
        :param line: linha do arquivo a ser lida
        :param advanced_debug: ativa o sistema de logging se definido para True
        :return: dict dentro do arquivo csv
        """
        with open(arquivo, mode='r') as file:
            if advanced_debug:
                self.logging.debug("lido dado no arquivo:" + str(arquivo))
            csvReader = csv.DictReader(file, delimiter=";")
            for i, row in enumerate(csvReader):
                if i == line:
                    retorno = row
                    file.flush()
                    file.close()
                    return retorno


class MYjson:
    def __init__(self, loggin_name="jsonManager", log_file="./jsonManager.log"):
        """
        classe para gerenciar arquivos json
        :param loggin_name: nome do log que foi definido para a classe,altere apenas em caso seja necessário criar multiplas insstancias da função
        :param log_file: nome do arquivo de log que foi definido para a classe,altere apenas em caso seja necessário criar multiplas insstancias da função
        """
        self.logging = loggingSystem(loggin_name, arquivo=log_file)

    def saveFile(self, arrayToSave, arquivo, advanced_debug=False):
        """
        função para salvar arquivos de tipo json
        :param arrayToSave: array de tipo dictionary para ser salvo no arquivo
        :param arquivo: nome do arquivo a ser acessadonome do arquivo onde será salvo
        :param advanced_debug: ativa o sistema de logging se definido para True
        :return:
        """
        with open(arquivo, 'w+') as file:
            if advanced_debug:
                self.logging.debug("salvo dado no arquivo:" + str(arquivo))
            for elemento in arrayToSave:
                file.write(json.dumps(elemento) + "\n")
            file.flush()
            file.close()

    def readFile(self, arquivo, advanced_debug=False):
        """

        :param arquivo: nome do arquivo a ser acessado
        :param advanced_debug: ativa o sistema de logging se definido para True
        :return: dictionary gerado a partir do arquivo lido
        """
        retorno = []
        with open(arquivo, 'r') as file:
            if advanced_debug:
                self.logging.debug("lido dado no arquivo:" + str(arquivo))
            linha = file.readline()
            while (linha != ""):
                retorno.append(json.loads(linha))
                linha = file.readline()
            file.close()
        return retorno


class loggingSystem:
    def __init__(self, name, arquivo='./arquivo.log', format='%(name)s - %(levelname)s - %(message)s',
                 level=logging.DEBUG):
        """

        :param name: nome do log a ser escrito no arquivo
        :param arquivo: nome do arquivo a ser utilizado
        :param format: formato do texto a ser inserido no output do log
        :param level: nivel de log padrão de saida
        """
        formatter = logging.Formatter(format)
        handler = logging.FileHandler(arquivo)
        handler.setFormatter(formatter)
        f = open(arquivo, "w+")
        f.write("")
        f.close()
        logger = logging.getLogger(name)
        logger.setLevel(level)
        logger.addHandler(handler)
        self.debug = logger.debug
        self.warning = logger.warning
        self.error = logger.error
        self.info = logger.info
        self.log = logger.log
        self.critical = logger.critical
        self.setlevel = logger.setLevel
        self.fatal = logger.fatal


class MYmysql:

    def __init__(self, user, passwd, host, database, loggin_name="mysql", log_file="./mysql.log"):
        """
        classe para gerenciar banco de dados mysql
        :param user: usuario mysql para acessar o banco de dados
        :param passwd: senha de acesso ao banco de dados
        :param host: endereço de acesso ao banco de dados
        :param database: nome do banco de dados a ser acessado
        :param loggin_name: nome do log que foi definido para a classe,altere apenas em caso seja necessário criar multiplas insstancias da função
        :param log_file: nome do arquivo de log que foi definido para a classe,altere apenas em caso seja necessário criar multiplas insstancias da função
        """
        self.logging = loggingSystem(loggin_name, arquivo=log_file)

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
        """

        :param dict:
        :param conversao:
        :param advanced_debug: ativa o sistema de logging se definido para True
        :return:
        """
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
        """

        :param headers:
        :param conversao:
        :param advanced_debug: ativa o sistema de logging se definido para True
        :return:
        """
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
        """

        :param tabelaBuscada:
        :param valorBuscado:
        :param colunaResultado:
        :param colunaBuscada:
        :param conversao:
        :param advanced_debug: ativa o sistema de logging se definido para True
        :return:
        """
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
        """

        :param dict:
        :param tabela:
        :param conversao:
        :param advanced_debug: ativa o sistema de logging se definido para True
        :return:
        """
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
        """

        :param dictArray:
        :param tabela:
        :param conversao:
        :return:
        """
        for dict in dictArray:
            self.addElement(dict, tabela, conversao)

    def deleteElement(self, tabela, parametros=[], searchRow=["id"]):
        """

        :param tabela:
        :param parametros:
        :param searchRow:
        :return:
        """
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
        """

        :param tabela:
        :param searchParametros:
        :param searchRow:
        :param advanced_debug: ativa o sistema de logging se definido para True
        :return:
        """
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


class MYntp:
    ntp = ntplib.NTPClient()

    def currentTime(self):
        """

        :return: hora atual baseada nos endereços do ntp.br
        """
        ntpAderesses = ["a.st1.ntp.br", "b.st1.ntp.br", "c.st1.ntp.br", "d.st1.ntp.br", "a.ntp.br", "b.ntp.br",
                        "c.ntp.br", "gps.ntp.br"]
        i = randint(0, len(ntpAderesses))
        try:
            time = self.ntp.request(ntpAderesses[i], version=3).orig_time
            return time
        except:
            return self.currentTime(self)

    def convertUnixToReadable(self, time="empty"):
        """
        :param time: horario a ser convertido de unixtime para hora legivel
        :return: horário convertido
        """
        if time == "empty":
            time = self.currentTime(self)
        return datetime.utcfromtimestamp(time).strftime('%Y-%m-%d %H:%M:%S')


class oauthAcess:
    def __init__(self, loggin_name="oauthAcess", log_file="./oauthAcess.log"):
        """
        classe para gerenciar arquivos csv
        :param loggin_name: nome do log que foi definido para a classe,altere apenas em caso seja necessário criar multiplas insstancias da função
        :param log_file: nome do arquivo de log que foi definido para a classe,altere apenas em caso seja necessário criar multiplas insstancias da função
        """
        self.logging = loggingSystem(loggin_name, arquivo=log_file)
        self.scope = ["https://spreadsheets.google.com/feeds",
                      "https://www.googleapis.com/auth/spreadsheets",
                      "https://www.googleapis.com/auth/drive.file",
                      "https://www.googleapis.com/auth/drive"]

    def get_cred_from_browser(self, CLIENT_ID="", CLIENT_SECRET="", json="", storage="", advanced_debug=False):
        """
        caso selecionado o storage não será exibido no navegador a página de login,
        caso contrário irá exibir abrir o navegador para realizar o login
        :param CLIENT_ID: client id do google oauth
        :param CLIENT_SECRET: client secret do google oauth
        :param json:arquivo contendo infos de login do google oauth
        :param storage: arquivo com as credenciais armazenadas do google oauth para não precisar abrir o navegador para realizar login
        :param advanced_debug: ativa o sistema de logging se definido para True
        :return:
        """

        def processar(storage, flow=None, advanced_debug=advanced_debug):
            if storage == "":
                storage = Storage('creds.data')
                credentials = run_flow(flow, storage)
                if advanced_debug:
                    self.logging.debug("login realizado sem titulo de arquivo de credencial")
            elif storage != "":
                storage = Storage(storage)
                credentials = run_flow(flow, storage)
                if advanced_debug:
                    self.logging.debug("login realizado com titulo de arquivo de credencial")

            return credentials

        try:
            if json == "" and CLIENT_ID != "" and CLIENT_SECRET != "":  # caso client_id e clinet_secret sejam inseridos
                flow = OAuth2WebServerFlow(client_id=CLIENT_ID,
                                           client_secret=CLIENT_SECRET,
                                           scope=self.scope,
                                           redirect_uri='http://example.com/auth_return')
                credentials = processar(storage, flow, advanced_debug=advanced_debug)
            elif json != "" and CLIENT_ID == "" and CLIENT_SECRET == "":  # caso seja passado arquivo json
                flow = client.flow_from_clientsecrets(json, self.scope)
                credentials = processar(storage, flow, advanced_debug=advanced_debug)
            elif storage != "" and json == "" and CLIENT_ID == "" and CLIENT_SECRET == "":  # caso seja carregado uma credencial armazenada
                credentials = Storage(storage).get()
            elif json == "" and CLIENT_ID == "" and CLIENT_SECRET == "" and storage == "":  # caso todos parametros estejam em branco
                raise Exception("invalid input")

            elif json != "" and CLIENT_ID != "" and CLIENT_SECRET != "" and storage != "":  # caso todos parametros estejam preenchidos
                raise Exception("invalid input")
            if advanced_debug:
                self.logging.debug("access_token: %s" % credentials.access_token)
            return credentials

        except Exception as exp:
            print(exp.args)
            self.logging.error(exp.args)

    def get_cred_automatic(self, json="", storage="", advanced_debug=False):
        try:
            if os.path.isfile(storage):
                if advanced_debug:
                    self.logging.debug("arquivo storage encontrado")
                credentials = self.get_cred_from_browser(storage=storage, advanced_debug=advanced_debug)
            elif not os.path.isfile(json):  # arquivo json não existe
                raise Exception("invalid input")

            elif not os.path.isfile(storage):  # arquivo storage n existe
                if advanced_debug:
                    self.logging.debug("não existe arquivo storage")
                credentials = self.get_cred_from_browser(json=json, storage=storage, advanced_debug=advanced_debug)

            if type(credentials) != client.OAuth2Credentials:
                return self.get_cred_automatic(json=json, advanced_debug=advanced_debug)
            else:
                return credentials
        except Exception as exp:
            print(exp.args)
            self.logging.error(exp.args)

    def get_cred_from_service_account_json(self, json):
        return ServiceAccountCredentials.from_json_keyfile_name(json, self.scope)


class MYG_Sheets:
    def __init__(self, credential=None, loggin_name="googleSheets", log_file="./googleSheets.log", wait_time=100,
                 json="", storage=""):
        """
        classe para gerenciar arquivos google sheets
        :param loggin_name: nome do log que foi definido para a classe,altere apenas em caso seja necessário criar multiplas insstancias da função
        :param log_file: nome do arquivo de log que foi definido para a classe,altere apenas em caso seja necessário criar multiplas insstancias da função
        :param credentialtype: pode ser "client" ou "service" dependendo do tipo de credencial o auth
        """
        if credential == None and json != "" and storage != "":
            self.creds = oauthAcess().get_cred_automatic(json=self.json, storage=self.storage)
        elif credential != None:
            self.creds = credential
        self.json = json
        self.storage = storage
        self.logging = loggingSystem(loggin_name, arquivo=log_file)

        self.client = gspread.authorize(self.creds)
        self.wait_time = wait_time

    def recreate_cert(self):
        self.creds = oauthAcess().get_cred_automatic(json=self.json, storage=self.storage)

    def add_page_header(self, sheet_id, page_number, headers, advanced_debug=False):
        try:
            sheet = self.load_sheet(sheet_id, advanced_debug=advanced_debug)
            page = self.select_page(sheet=sheet, page_number=page_number, advanced_debug=advanced_debug)
            self.replace_data_row(sheet, page, headers, 1)
        except:
            traceback.print_exc()
            Utility().wait(self.wait_time)
            self.add_page_header(sheet_id, page_number, headers, advanced_debug=advanced_debug)

    """ relacionado as paginas """

    def select_page(self, sheet, page_number, advanced_debug=False):
        page = None
        try:
            if type(page_number) == type(""):
                return sheet.worksheet(page_number)
            elif type(page_number) == type(1):
                return sheet.get_worksheet(page_number)
            elif type(page_number) == gspread.models.Worksheet:
                return page_number
            if advanced_debug:
                print("pagina de id " + str(page.id) + "e nome " + str(page.title) + " selecionada")
                self.logging.debug("pagina de id " + str(page.id) + "e nome " + str(page.title) + " selecionada")
        except gspread.exceptions.APIError as exp:
            traceback.print_exc()
            # print(exp.args[0]["code"])
            if exp.args[0]["code"] == 429:
                Utility().wait(self.wait_time)
                return self.select_page(sheet, page_number, advanced_debug)
            elif exp.args[0]["code"] == 401:
                # Utility().wait(self.wait_time)
                self.recreate_cert()
                return self.select_page(sheet, page_number, advanced_debug)
            else:
                print(exp.arg[0])
                self.logging.warning(exp.args[0])
                traceback.print_exc()
        except Exception as exp:
            print(str(exp) + " select_page")
            self.logging.error(str(exp) + " select_page")

    def add_page(self, sheet_id, page_name, minimum_col=24, minimum_row=1, advanced_debug=False):
        """

        :param sheet_id: id da planilha google sheets
        :param page_name: nome da nova pagina da planilha
        :param minimum_col:
        :param minimum_row:
        :param advanced_debug: ativa o sistema de logging se definido para True
        :return: id da nova página criada
        """
        try:
            sheet = self.load_sheet(sheet_id, advanced_debug=advanced_debug)
            if sheet.worksheet(page_name) != None:
                id = sheet.worksheet(page_name)
            else:
                id = sheet.add_worksheet(title=page_name, rows=minimum_row, cols=minimum_col).id
            if advanced_debug:
                self.logging.debug("foi criada uma página nova com titulo " + str(page_name) + " seu id é " + str(id))
            return id
        except gspread.exceptions.APIError as exp:
            # print(exp.args[0]["code"])
            id = None
            if exp.args[0]["code"] == 429:
                Utility().wait(self.wait_time)
                id = self.add_page(sheet_id, page_name, minimum_col, minimum_row, advanced_debug)
            elif exp.args[0]["code"] == 400:
                id = self.select_page(sheet_id, page_name, advanced_debug).id
            elif exp.args[0]["code"] == 401:
                # Utility().wait(self.wait_time)
                self.recreate_cert()
                id = self.select_page(sheet_id, page_name, advanced_debug).id
            else:
                print(exp.arg[0])
                self.logging.warning(exp.args[0])
                traceback.print_exc()
            return id
        except Exception as exp:
            print(str(exp) + " add_page")
            self.logging.error(str(exp) + " add_page")

    def delete_page(self, sheet_id, page_number, advanced_debug=False):
        """

        :param sheet_id: id da planilha google sheets
        :param page_number: pagina da planilha a ser editada numero da página da planilha
        :param advanced_debug: ativa o sistema de logging se definido para True
        :return:
        """
        try:
            sheet = self.load_sheet(sheet_id, advanced_debug=advanced_debug)
            page = self.select_page(sheet, page_number)
            name = page.title
            sheet.del_worksheet(page)
            if advanced_debug:
                self.logging.debug("pagina numero " + str(page_number) + "com nome " + str(name) + " foi deletada")
        except gspread.exceptions.APIError as exp:
            traceback.print_exc()
            # print(exp.args[0]["code"])
            if exp.args[0]["code"] == 429:
                Utility().wait(self.wait_time)
                self.delete_page(sheet_id, page_number, advanced_debug)
            elif exp.args[0]["code"] == 401:
                # Utility().wait(self.wait_time)
                self.recreate_cert()
                self.delete_page(sheet_id, page_number, advanced_debug)
            else:
                self.logging.warning(exp.args[0])
                traceback.print_exc()
        except Exception as exp:
            print(str(exp) + " delete_page")
            self.logging.error(str(exp) + " delete_page")

    def recreate_page(self, sheet_id, page_number, advanced_debug=False):
        try:
            sheet = self.load_sheet(sheet_id=sheet_id, advanced_debug=advanced_debug)
            page = self.select_page(sheet, page_number=page_number, advanced_debug=advanced_debug)
            page_col = page.col_count
            page_row = page.row_count
            page_name = page.title
            self.delete_page(sheet, page, advanced_debug=advanced_debug)
            page = self.add_page(sheet, page_name, minimum_col=page_col, minimum_row=page_row,
                                 advanced_debug=advanced_debug)
            return page
        except gspread.exceptions.APIError as exp:
            traceback.print_exc()
            # print(exp.args[0]["code"])
            if exp.args[0]["code"] == 429:
                Utility().wait(self.wait_time)
                page = self.add_page(sheet, page_name, minimum_col=page_col, minimum_row=page_row,
                                     advanced_debug=advanced_debug)
            elif exp.args[0]["code"] == 401:
                # Utility().wait(self.wait_time)
                self.recreate_cert()
                page = self.add_page(sheet, page_name, minimum_col=page_col, minimum_row=page_row,
                                     advanced_debug=advanced_debug)
            else:
                self.logging.warning(exp.args[0])
                traceback.print_exc()
            return page
        except Exception as exp:
            print(str(exp) + " delete_page")
            self.logging.error(str(exp) + " delete_page")

    def retrive_page_data(self, sheet_id, page_number, head=1, advanced_debug=False):
        return self.retrive_range_data(sheet_id=sheet_id, page_number=page_number, advanced_debug=advanced_debug,
                                       head=head)

    """  relacionado a planilha  """

    def add_reader_sheet(self, sheet_id, email, advanced_debug=False):
        """

        :param sheet_id: id da planilha google sheets
        :param email:
        :param advanced_debug: ativa o sistema de logging se definido para True
        :return:
        """
        try:
            self.client.insert_permission(sheet_id, email, perm_type="user", role="reader")
            if advanced_debug:
                self.logging.debug("usuario " + str(email) + " adicionada nova pessoa com permissao de leitura")
        except gspread.exceptions.APIError as exp:
            traceback.print_exc()
            # print(exp.args[0]["code"])
            if exp.args[0]["code"] == 429:
                Utility().wait(self.wait_time)
                self.add_reader_sheet(sheet_id, email, advanced_debug)
            elif exp.args[0]["code"] == 401:
                # Utility().wait(self.wait_time)
                self.recreate_cert()
                self.add_reader_sheet(sheet_id, email, advanced_debug)
            else:
                print(exp.args[0])
                self.logging.warning(exp.args[0])
                traceback.print_exc()
        except Exception as exp:
            print(str(exp) + " add_reader_sheet")
            self.logging.error(str(exp) + " add_reader_sheet")

    def add_writer_sheet(self, sheet_id, email, advanced_debug=False):
        """

        :param sheet_id: id da planilha google sheets
        :param email:
        :param advanced_debug: ativa o sistema de logging se definido para True
        :return:
        """
        try:
            self.client.insert_permission(sheet_id, email, perm_type="user", role="writer")
            if advanced_debug:
                self.logging.debug("usuario " + str(email) + " adicionada nova pessoa com permissao de escrita")
        except gspread.exceptions.APIError as exp:
            traceback.print_exc()
            # print(exp.args[0]["code"])
            if exp.args[0]["code"] == 429:
                Utility().wait(self.wait_time)
                self.add_writer_sheet(sheet_id, email, advanced_debug)
            elif exp.args[0]["code"] == 401:
                # Utility().wait(self.wait_time)
                self.recreate_cert()
                self.add_writer_sheet(sheet_id, email, advanced_debug)
            else:
                print(exp.args[0])
                self.logging.warning(exp.args[0])
                traceback.print_exc()
        except Exception as exp:
            print(str(exp) + " add_writer_sheet")
            self.logging.error(str(exp) + " add_writer_sheet")

    def change_owner_sheet(self, sheet_id, email, advanced_debug=False):
        """

        :param sheet_id: id da planilha google sheets
        :param email:
        :param advanced_debug: ativa o sistema de logging se definido para True
        :return:
        """
        try:
            self.client.insert_permission(sheet_id, email, perm_type="user", role="owner")
            if advanced_debug:
                self.logging.debug("arquivo teve propriedade movida para o usuario " + str(email))
        except gspread.exceptions.APIError as exp:
            traceback.print_exc()
            # print(exp.args[0]["code"])
            if exp.args[0]["code"] == 429:
                Utility().wait(self.wait_time)
                self.change_owner_sheet(sheet_id, email, advanced_debug)
            elif exp.args[0]["code"] == 401:
                # Utility().wait(self.wait_time)
                self.recreate_cert()
                self.change_owner_sheet(sheet_id, email, advanced_debug)
            else:
                print(exp.args[0])
                self.logging.warning(exp.args[0])
                traceback.print_exc()
        except Exception as exp:
            print(str(exp) + " change_owner_sheet")
            self.logging.error(str(exp) + " change_owner_sheet")

    def change_sheet_to_public_read(self, sheet_id, advanced_debug=False):
        """

        :param sheet_id: id da planilha google sheets
        :param advanced_debug: ativa o sistema de logging se definido para True
        :return:
        """
        try:
            self.client.insert_permission(sheet_id, value=None, perm_type="anyone", role="reader")
            if advanced_debug:
                self.logging.debug(
                    "a planilha com id " + str(sheet_id) + " teve permissão de escrita publica habilitada")
        except gspread.exceptions.APIError as exp:
            traceback.print_exc()
            # print(exp.args[0]["code"])
            if exp.args[0]["code"] == 429:
                Utility().wait(self.wait_time)
                self.change_sheet_to_public_read(sheet_id, advanced_debug)
            elif exp.args[0]["code"] == 401:
                # Utility().wait(self.wait_time)
                self.recreate_cert()
                self.change_sheet_to_public_read(sheet_id, advanced_debug)
            else:
                print(exp.args[0])
                self.logging.warning(exp.args[0])
                traceback.print_exc()
        except Exception as exp:
            print(str(exp) + " change_sheet_to_public_read")
            self.logging.error(str(exp) + " change_sheet_to_public_read")

    def change_sheet_to_public_write(self, sheet_id, advanced_debug=False):
        """

        :param sheet_id: id da planilha google sheets
        :param advanced_debug: ativa o sistema de logging se definido para True
        :return:
        """
        try:
            self.client.insert_permission(sheet_id, value=None, perm_type="anyone", role="writer")
            if advanced_debug:
                self.logging.debug(
                    "a planilha com id " + str(sheet_id) + " teve permissão de leitura publica habilitada")
        except gspread.exceptions.APIError as exp:
            traceback.print_exc()
            # print(exp.args[0]["code"])
            if exp.args[0]["code"] == 429:
                Utility().wait(self.wait_time)
                self.change_sheet_to_public_write(sheet_id, advanced_debug)
            elif exp.args[0]["code"] == 401:
                # Utility().wait(self.wait_time)
                self.recreate_cert()
                self.change_sheet_to_public_write(sheet_id, advanced_debug)
            else:
                print(exp.args[0])
                self.logging.warning(exp.args[0])
                traceback.print_exc()
        except Exception as exp:
            print(str(exp) + " change_sheet_to_public_write")
            self.logging.error(str(exp) + " change_sheet_to_public_write")

    def load_sheet(self, sheet_id, advanced_debug=False):
        try:
            if type(sheet_id) == gspread.models.Spreadsheet:
                sheet = sheet_id
            elif type(sheet_id) == type(""):
                sheet = self.client.open_by_key(sheet_id)
            if advanced_debug:
                self.logging.debug("planilha carregada: " + str(type(sheet)))
            return sheet
        except gspread.exceptions.APIError as exp:
            traceback.print_exc()
            # print(exp.args[0]["code"])
            if exp.args[0]["code"] == 429:
                Utility().wait(self.wait_time)
                self.load_sheet(sheet_id, advanced_debug=advanced_debug)
            elif exp.args[0]["code"] == 401:
                # Utility().wait(self.wait_time)
                self.recreate_cert()
                self.load_sheet(sheet_id, advanced_debug=advanced_debug)
            else:
                self.logging.warning(exp.args[0])
                traceback.print_exc()
        except Exception as exp:
            print(str(exp) + " load_sheet")
            self.logging.error(str(exp) + " load_sheet")

    def create_sheet(self, sheet_name, owner=None, public_read=False, public_write=False, advanced_debug=False):
        """

        :param sheet_name: nome da planilha google sheets
        :param owner: email do novo dono da planilha
        :param public_read: habilita o arquivo para leitura pública
        :param public_write: habilita o arquivo para escrita púlica
        :param advanced_debug: ativa o sistema de logging se definido para True
        :return: o id da nova planilha
        """
        try:
            id = self.client.create(sheet_name).id
            if advanced_debug:
                self.logging.debug("planilha criada com nome de " + str(sheet_name) + " e id de " + str(id))
            if public_read:
                self.change_sheet_to_public_read(id, advanced_debug=advanced_debug)
            if public_write:
                self.change_sheet_to_public_write(id, advanced_debug=advanced_debug)
            if owner != None:
                self.change_owner_sheet(sheet_id=id, email=owner, advanced_debug=advanced_debug)
            return id
        except gspread.exceptions.APIError as exp:
            traceback.print_exc()
            # print(exp.args[0]["code"])
            if exp.args[0]["code"] == 429:
                Utility().wait(self.wait_time)
                id = self.create_sheet(sheet_name, owner, public_read, public_write, advanced_debug)
            elif exp.args[0]["code"] == 401:
                # Utility().wait(self.wait_time)
                self.recreate_cert()
                id = self.create_sheet(sheet_name, owner, public_read, public_write, advanced_debug)
            else:
                self.logging.warning(exp.args[0])
                traceback.print_exc()
            return id
        except Exception as exp:
            print(str(exp) + " create_sheet")
            self.logging.error(str(exp) + " create_sheet")

    def delete_sheet(self, sheet_id, advanced_debug=False):
        """

        :param sheet_id: id da planilha google sheets
        :param advanced_debug: ativa o sistema de logging se definido para True
        :return:
        """
        try:
            name = self.load_sheet(sheet_id, advanced_debug=advanced_debug).title
            self.client.del_spreadsheet(sheet_id)
            if advanced_debug:
                self.logging.debug("a planilha com id " + str(sheet_id) + " e nome de " + str(name) + " foi deletada")
        except gspread.exceptions.APIError as exp:
            traceback.print_exc()
            # print(exp.args[0]["code"])
            if exp.args[0]["code"] == 429:
                Utility().wait(self.wait_time)
                self.delete_sheet(sheet_id, advanced_debug)
            elif exp.args[0]["code"] == 401:
                # Utility().wait(self.wait_time)
                self.recreate_cert()
                self.delete_sheet(sheet_id, advanced_debug)
            else:
                self.logging.warning(exp.args[0])
                traceback.print_exc()
        except Exception as exp:
            print(str(exp) + " delete_sheet")
            self.logging.error(str(exp) + " delete_sheet")

    def sheet_to_dict(self, sheet_id, head=1, advanced_debug=False):
        retorno = {}
        try:
            sheet = self.load_sheet(sheet_id, advanced_debug=advanced_debug)
            for page in sheet.worksheets():
                if advanced_debug:
                    self.logging.debug(page.title)
                try:
                    retorno[page.title] = self.retrive_page_data(sheet, page, head=head, advanced_debug=advanced_debug)
                except gspread.exceptions.APIError as exp:
                    # print(exp.args[0]["code"])
                    retorno[page.title] = {}
                    if exp.args[0]["code"] == 429:
                        while retorno[page.title] == {}:
                            Utility().wait(self.wait_time)
                            retorno[page.title] = self.retrive_page_data(sheet, page, head=head,
                                                                         advanced_debug=advanced_debug)
            return retorno
        except gspread.exceptions.APIError as exp:
            # print(exp.args[0]["code"])
            if exp.args[0]["code"] == 429:
                Utility().wait(self.wait_time)
                sheet = self.sheet_to_dict(sheet_id, head, advanced_debug)
            elif exp.args[0]["code"] == 401:
                # Utility().wait(self.wait_time)
                self.recreate_cert()
                sheet = self.sheet_to_dict(sheet_id, head, advanced_debug)
            else:
                self.logging.warning(exp.args[0])
                traceback.print_exc()
            return sheet
        except Exception as exp:
            print(str(exp) + " retrive_data")
            self.logging.error(str(exp) + " retrive_data")

    def dict_to_sheet(self, sheet_id, dictionary={}, advanced_debug=False):
        try:
            sheet = self.load_sheet(sheet_id, advanced_debug=advanced_debug)
            for page in dictionary:
                self.add_page(sheet_id,page, advanced_debug=advanced_debug, minimum_col=len(dictionary[page][0].keys())+1, minimum_row=len(dictionary[page])+1)
                self.add_multiple_data_row(sheet, page, dictionary[page], advanced_debug=advanced_debug)
        except gspread.exceptions.APIError as exp:
            # print(exp.args[0]["code"])
            if exp.args[0]["code"] == 429:
                Utility().wait(self.wait_time)
                self.dict_to_sheet(sheet_id, dictionary, advanced_debug)
            elif exp.args[0]["code"] == 401:
                # Utility().wait(self.wait_time)
                self.recreate_cert()
                self.dict_to_sheet(sheet_id, dictionary, advanced_debug)
            else:
                self.logging.warning(exp.args[0])
                traceback.print_exc()
        except Exception as exp:
            print(str(exp) + " retrive_data")
            self.logging.error(str(exp) + " retrive_data")

    """  relacionado aos dados  """

    def update_data_range(self, sheet_id, page_number, list_of_row: [int], list_of_col: [int], list_of_values: [],
                          advanced_debug=False):
        """

        :param sheet_id: id da planilha google sheets
        :param page_number: pagina da planilha a ser editada
        :param list_of_row: lista com as linhas das celulas que serão editadas
        :param list_of_col: lista com as colunas das celulas que serão editadas
        :param list_of_values: lista com valores novos das celulas que serão editadas
        as tres listas devem ser encaradas como uma matriz 2d unica de 3 colunas
        :param advanced_debug: ativa o sistema de logging se definido para True
        :return:
        """
        sheet = self.load_sheet(sheet_id, advanced_debug=advanced_debug)
        page = self.select_page(sheet=sheet, page_number=page_number, advanced_debug=advanced_debug)
        try:
            if len(list_of_col) != len(list_of_row) != len(list_of_values):
                raise Exception("invalid amount of elemnts on lists")
            else:
                cells = []
                for i in range(0, len(list_of_values)):
                    cells.append(Cell(row=list_of_row[i], col=list_of_col[i], value=list_of_values[i]))
                page.update_cells(cells)
                if advanced_debug:
                    self.logging.debug("foi concluida a inserção de dados na região definida")
        except gspread.exceptions.APIError as exp:
            # print(exp.args[0]["code"])
            if exp.args[0]["code"] == 429:
                Utility().wait(self.wait_time)
                self.update_data_range(sheet_id, page_number, list_of_row, list_of_col, list_of_values, advanced_debug)
            elif exp.args[0]["code"] == 400:
                for i in list_of_row:
                    while page.row_count < i:
                        self.add_data_row(sheet_id, page_number, {}, advanced_debug=advanced_debug)
                for i in list_of_col:
                    while page.col_count < i:
                        page.add_cols(i - page.col_count)
                self.update_data_range(sheet_id, page_number, list_of_row, list_of_col, list_of_values, advanced_debug)
            elif exp.args[0]["code"] == 401:
                # Utility().wait(self.wait_time)
                self.recreate_cert()
                self.update_data_range(sheet_id, page_number, list_of_row, list_of_col, list_of_values, advanced_debug)
            else:
                print(exp.args[0])
                self.logging.warning(exp.args[0])
                traceback.print_exc()
        except Exception as exp:
            print(" update_data_range")
            print(exp)
            self.logging.error("update_data_range")
            self.logging.error(exp)
            traceback.print_exc()

    def retrive_range_data(self, sheet_id, page_number, import_range="all", head=1, advanced_debug=False):
        """

        :param sheet_id: id da planilha google sheets
        :param page_number: pagina da planilha a ser editada
        :param import_range:
        :param advanced_debug: ativa o sistema de logging se definido para True
        :return:
        """
        try:
            sheet = self.load_sheet(sheet_id, advanced_debug=advanced_debug)
            page = self.select_page(sheet=sheet, page_number=page_number, advanced_debug=advanced_debug)
            if import_range == "all":
                if advanced_debug:
                    self.logging.debug("feita consulta na planilha: " + str(sheet_id) + " na página de nome " + str(
                        page.title) + " e numero " + str(
                        page.id) + " onde todos os dados foram retornados")
                return page.get_all_records(head=head)
            else:
                if advanced_debug:
                    self.logging.debug("feita consulta na planilha: " + str(sheet_id) + " na página de nome " + str(
                        sheet.get_worksheet(page_number).title) + " e numero " + str(
                        sheet.get_worksheet(page_number).id) + " onde foram retornados os valores do intervalo: " + str(
                        import_range))
                return page.range(import_range)
        except gspread.exceptions.APIError as exp:
            # print(exp.args[0]["code"])
            if exp.args[0]["code"] == 429:
                Utility().wait(self.wait_time)
                return self.retrive_range_data(sheet_id, page_number, import_range, head, advanced_debug)
            elif exp.args[0]["code"] == 401:
                # Utility().wait(self.wait_time)
                self.recreate_cert()
                return self.retrive_range_data(sheet_id, page_number, import_range, head, advanced_debug)
            else:
                self.logging.warning(exp.args[0])
                traceback.print_exc()
        except Exception as exp:
            print(str(exp) + " retrive_data")
            self.logging.error(str(exp) + " retrive_data")

    def update_data_cell(self, sheet_id, page_number, cell_cood, new_value, advanced_debug=False):
        """

        :param sheet_id: id da planilha google sheets
        :param page_number: pagina da planilha a ser editada
        :param cell_cood: coordenada da celula a ser manipulada,podendo ser coordenadas [x,y] em formato de array 1d,ou string padrão de excell "A1"
        :param new_value: novo valor a ser inserido
        :param advanced_debug: ativa o sistema de logging se definido para True
        :return:
        """
        try:
            sheet = self.load_sheet(sheet_id, advanced_debug=advanced_debug)
            page = self.select_page(sheet=sheet, page_number=page_number, advanced_debug=advanced_debug)
            try:
                if isinstance(cell_cood, str):
                    page.update_acell(cell_cood, new_value)
                    if advanced_debug:
                        self.logging.debug("o intervalo da planilha foi atualizado")
                elif type(cell_cood) == type([]) and len(cell_cood) == 2:
                    page.update_cell(cell_cood[0], cell_cood[1], new_value)
                    if advanced_debug:
                        self.logging.debug("o intervalo da planilha foi atualizado")
                else:
                    raise Exception('invalid input cell_cood')
            except Exception as error:
                print(error.args)
            self.logging.error(error.args)
        except gspread.exceptions.APIError as exp:
            traceback.print_exc()
            # print(exp.args[0]["code"])
            if exp.args[0]["code"] == 429:
                Utility().wait(self.wait_time)
                self.update_data_cell(sheet_id, page_number, cell_cood, new_value, advanced_debug)
            elif exp.args[0]["code"] == 401:
                # Utility().wait(self.wait_time)
                self.recreate_cert()
                self.update_data_cell(sheet_id, page_number, cell_cood, new_value, advanced_debug)
            else:
                print(exp.args[0])
                self.logging.warning(exp.args[0])
                traceback.print_exc()
        except Exception as exp:
            print(" update_data_cell")
            print(exp)
            self.logging.error("update_data_cell")
            self.logging.error(exp)
            traceback.print_exc()

    def delete_data_cell(self, sheet_id, page_number, cell_cood, advanced_debug=False):
        """

        :param sheet_id: id da planilha google sheets
        :param page_number: pagina da planilha a ser editada
        :param cell_cood: coordenada da celula a ser manipulada,podendo ser coordenadas [x,y] em formato de array 1d,ou string padrão de excell "A1"
        :param advanced_debug: ativa o sistema de logging se definido para True
        :return:
        """
        try:
            sheet = self.load_sheet(sheet_id, advanced_debug=advanced_debug)
            page = self.select_page(sheet=sheet, page_number=page_number, advanced_debug=advanced_debug)
            try:
                if isinstance(cell_cood, str):
                    page.update_acell(cell_cood, "")
                    if advanced_debug:
                        self.logging.debug("a celula de endereco " + str(cell_cood) + " foi limpa")
                elif type(cell_cood) == type([]) and len(cell_cood) == 2:
                    page.update_cell(cell_cood[0], cell_cood[1], "")
                    if advanced_debug:
                        self.logging.debug(
                            "a celula de coordenada [" + str(cell_cood[0]) + "," + str(cell_cood[1]) + "] foi limpa")
                else:
                    raise Exception('invalid input cell_cood', cell_cood)
            except Exception as exp:
                print(" update_data_cell")
                print(exp)

                self.logging.error("update_data_cell")
                self.logging.error(exp)
                traceback.print_exc()
        except gspread.exceptions.APIError as exp:
            traceback.print_exc()
            # print(exp.args[0]["code"])
            if exp.args[0]["code"] == 429:
                Utility().wait(self.wait_time)
                self.delete_data_cell(sheet_id, page_number, cell_cood, advanced_debug)
            elif exp.args[0]["code"] == 401:
                # Utility().wait(self.wait_time)
                self.recreate_cert()
                self.delete_data_cell(sheet_id, page_number, cell_cood, advanced_debug)
            else:
                print(exp.args[0])
                self.logging.warning(exp.args[0])
                traceback.print_exc()
        except Exception as exp:
            print(str(exp) + " delete_data_cell")
            self.logging.error(str(exp) + " delete_data_cell")

    """  relacionado as linhas  """

    def add_data_row(self, sheet_id, page_number, elemento: dict, row_id: int = 0, substitute=False,
                     advanced_debug=False):
        """

        :param sheet_id: id da planilha google sheets
        :param page_number: pagina da planilha a ser editada
        :param elemento: dictionary com os elementos a serem inseridos
        :param row_id: numero da linha a ser inserida,se em branco apenas insere ao final do arquivo
        :param advanced_debug: ativa o sistema de logging se definido para True
        :param substitute: se a linha selecionada por row_id não estiver em branco substitui ela,caso contrário ela será passada para a linha a baixo
        :return:
        """
        if advanced_debug:
            print("sheet id " + str(sheet_id))
            print("page_number " + str(page_number))
            print("row_id " + str(row_id))
            print("substitute " + str(substitute))
            print("elemento " + str(elemento))
        try:
            if type(elemento) == type([]):
                raise Exception("invalid data type,use add_multiple_data_row instead")
            sheet = self.load_sheet(sheet_id, advanced_debug=advanced_debug)
            page = self.select_page(sheet=sheet, page_number=page_number, advanced_debug=advanced_debug)
            headers = page.row_values(1)
            if row_id != 0:
                if page.row_count < row_id:
                    raise Exception("unreachable row " + str(row_id) + " is not into the " + str(page.row_count))
            if advanced_debug:
                print("indo conferir headers")
            reordened = {}
            if headers != []:
                for element in elemento:
                    if element not in headers:
                        raise Exception('header incompatible')
            else:
                self.add_page_header(self, sheet_id, page_number, elemento.keys(), advanced_debug)

            for header in headers:
                reordened[header] = elemento[header]
            elemento = []
            for reorder in reordened:
                elemento.append(reordened[reorder])
            if row_id != 0:
                vazio = True
                for value in page.row_values(row_id):
                    if value != "":
                        vazio = False
                        break
                if substitute and not vazio:
                    self.delete_data_row(sheet_id, page_number, row_id, advanced_debug=False)
                page.insert_row(elemento, index=row_id)
                if advanced_debug:
                    self.logging.debug("a linha de numero " + str(row_id) + " foi adicionada")
            else:
                page.append_row(elemento)
                if advanced_debug:
                    self.logging.debug("a linha foi adicionada ao final do arquivo")
        except gspread.exceptions.APIError as exp:
            traceback.print_exc()
            # print(exp.args[0]["code"])
            if exp.args[0]["code"] == 429:
                Utility().wait(self.wait_time)
                self.add_data_row(sheet_id, page_number, elemento, row_id, substitute, advanced_debug)
            elif exp.args[0]["code"] == 401:
                # Utility().wait(self.wait_time)
                self.recreate_cert()
                self.add_data_row(sheet_id, page_number, elemento, row_id, substitute, advanced_debug)
            else:
                print(exp.args[0])
                self.logging.warning(exp.args[0])
                traceback.print_exc()
        except Exception as exp:
            print(" add_data_row")
            print(exp)
            self.logging.error("add_data_row")
            self.logging.error(exp)
            traceback.print_exc()

    def retrive_data_row(self, sheet_id, page_number, row_id, advanced_debug=False):
        try:
            sheet = self.load_sheet(sheet_id, advanced_debug=advanced_debug)
            page = self.select_page(sheet=sheet, page_number=page_number, advanced_debug=advanced_debug)
            if advanced_debug:
                self.logging.debug("feita consulta na planilha: " + str(sheet_id) + " na página de nome " + str(
                    page.title) + " e numero " + str(
                    page.id) + " onde todos os dados foram retornados")
            return page.row_values(row_id)
        except gspread.exceptions.APIError as exp:
            traceback.print_exc()
            # print(exp.args[0]["code"])
            if exp.args[0]["code"] == 429:
                Utility().wait(self.wait_time)
                row = self.retrive_data_row(sheet_id, page_number, row_id, advanced_debug)
            elif exp.args[0]["code"] == 401:
                # Utility().wait(self.wait_time)
                self.recreate_cert()
                row = self.retrive_data_row(sheet_id, page_number, row_id, advanced_debug)
            else:
                self.logging.warning(exp.args[0])
                traceback.print_exc()
            return row
        except Exception as exp:
            print(" retrive_data_row")
            print(exp)
            self.logging.error("retrive_data_row")
            self.logging.error(exp)
            traceback.print_exc()

    def add_multiple_data_row(self, sheet_id, page_number, elementos: [dict], row_id=None, substitute=False,
                              advanced_debug=False):
        """

        :param sheet_id: id da planilha google sheets
        :param page_number: pagina da planilha a ser editada
        :param elemento: dictionary com os elementos a serem inseridos
        :param row_id: numero da linha inicial a ser inserida,se ela ou qualquer uma subjacente estiver em branco apenas insere ao final do arquivo
        :param advanced_debug: ativa o sistema de logging se definido para True
        :param substitute: se a linha selecionada por row_id não estiver em branco substitui ela,caso contrário ela será passada para a linha a baixo,isso é valido para todas as linhas
        :return:
        """
        try:
            sheet_id = self.load_sheet(sheet_id, advanced_debug=advanced_debug)
            page_number = self.select_page(sheet_id, page_number, advanced_debug=advanced_debug)
            for elemento in elementos:
                self.add_data_row(sheet_id=sheet_id, page_number=page_number, elemento=elemento, row_id=row_id,
                                  substitute=substitute, advanced_debug=advanced_debug)
            if type(row_id) == type(1):
                row_id = row_id + 1
        except gspread.exceptions.APIError as exp:
            traceback.print_exc()
            # print(exp.args[0]["code"])
            if exp.args[0]["code"] == 429:
                Utility().wait(self.wait_time)
                self.add_multiple_data_row(sheet_id, page_number, elementos, row_id, substitute, advanced_debug)
            elif exp.args[0]["code"] == 401:
                # Utility().wait(self.wait_time)
                self.recreate_cert()
                self.add_multiple_data_row(sheet_id, page_number, elementos, row_id, substitute, advanced_debug)
            else:
                print(exp.args[0])
                self.logging.warning(exp.args[0])
                traceback.print_exc()
        except Exception as exp:
            print(" add_multiple_data_row")
            print(exp)
            self.logging.error("add_multiple_data_row")
            self.logging.error(exp)
            traceback.print_exc()

    def replace_data_row(self, sheet_id, page_number, elementos: [], row_id, advanced_debug=False):
        try:
            sheet_id = self.load_sheet(sheet_id, advanced_debug=advanced_debug)
            page_number = self.select_page(sheet_id, page_number, advanced_debug=advanced_debug)
            rows = []
            cols = []
            for i in range(0, len(elementos)):
                rows.append(row_id)
                cols.append(i + 1)
            self.update_data_range(sheet_id=sheet_id, page_number=page_number, list_of_row=rows,
                                   list_of_col=cols, list_of_values=elementos, advanced_debug=advanced_debug)
        except gspread.exceptions.APIError as exp:
            # print(exp.args[0]["code"])
            if exp.args[0]["code"] == 429:
                Utility().wait(self.wait_time)
                self.replace_data_row(sheet_id, page_number, elementos, row_id, advanced_debug)
            elif exp.args[0]["code"] == 401:
                # Utility().wait(self.wait_time)
                self.recreate_cert()
                self.replace_data_row(sheet_id, page_number, elementos, row_id, advanced_debug)
            else:
                self.logging.warning(exp.args[0])
                traceback.print_exc()
        except Exception as exp:
            print(
                "update_data_row sheet_id:" + str(sheet_id) + " page_number:" + str(page_number) + " elementos:" + str(
                    elementos) + " row_id:" + str(row_id))
            print(exp)
            traceback.print_exc()
            self.logging.error(
                "update_data_row sheet_id:" + str(sheet_id) + " page_number:" + str(page_number) + " elemento:" + str(
                    elementos) + " row_id:" + str(row_id))
            self.logging.error(exp)
            traceback.print_exc()

    def delete_data_row(self, sheet_id, page_number, row_id, advanced_debug=False):
        """

        :param sheet_id: id da planilha google sheets
        :param page_number: pagina da planilha a ser editada
        :param row_id: numero da linha a ser manipulada
        :param advanced_debug: ativa o sistema de logging se definido para True
        :return:
        """
        try:
            sheet = self.load_sheet(sheet_id, advanced_debug=advanced_debug)
            page = self.select_page(sheet, page_number, advanced_debug=advanced_debug)
            page.delete_row(row_id)
            if advanced_debug:
                self.logging.debug("a linha de numero " + str(row_id) + " foi apagada")
        except gspread.exceptions.APIError as exp:
            traceback.print_exc()
            # print(exp.args[0]["code"])
            if exp.args[0]["code"] == 429:
                Utility().wait(self.wait_time)
                self.delete_data_row(sheet_id, page_number, row_id, advanced_debug)
            elif exp.args[0]["code"] == 401:
                # Utility().wait(self.wait_time)
                self.recreate_cert()
                self.delete_data_row(sheet_id, page_number, row_id, advanced_debug)
            else:
                print(exp.args[0])
                self.logging.warning(exp.args[0])
                traceback.print_exc()
        except Exception as exp:
            print(" delete_data_row")
            print(exp)
            self.logging.error("delete_data_row")
            self.logging.error(exp)
            traceback.print_exc()

    def update_data_row(self, sheet_id, page_number, elemento: dict, row_id: int = 0, advanced_debug=False):
        try:
            sheet = self.load_sheet(sheet_id, advanced_debug=advanced_debug)
            page = self.select_page(sheet=sheet, page_number=page_number, advanced_debug=advanced_debug)
            if advanced_debug:
                self.logging.debug("feita consulta na planilha: " + str(sheet_id) + " na página de nome " +
                                   str(page.title) + " e numero " + str(
                    page.id) + " onde todos os dados foram retornados")
            array = []
            array.append(elemento)
            objetos = DictTools().dict_to_cood_array(dictionary_array=array, advanced_debug=advanced_debug,
                                                     element_orientation="col")
            rows = []
            for i in range(0, len(objetos["x"])):
                rows.append(row_id)
            for i in range(0, len(objetos["y"])):
                objetos["y"][i] = objetos["y"][i] + 1
            if advanced_debug:
                print("list_of_row: " + str(rows))
                print("list_of_col: " + str(objetos["y"]))
                print("list_of_values: " + str(objetos["value"]))
            self.update_data_range(sheet_id=sheet_id, page_number=page, list_of_row=rows,
                                   list_of_col=objetos["y"], list_of_values=objetos["value"],
                                   advanced_debug=advanced_debug)
        except gspread.exceptions.APIError as exp:
            # print(exp.args[0]["code"])
            if exp.args[0]["code"] == 429:
                Utility().wait(self.wait_time)
                row = self.update_data_row(sheet_id, page_number, elemento, row_id, advanced_debug)
            elif exp.args[0]["code"] == 401:
                # Utility().wait(self.wait_time)
                self.recreate_cert()
                row = self.update_data_row(sheet_id, page_number, elemento, row_id, advanced_debug)
            else:
                self.logging.warning(exp.args[0])
                traceback.print_exc()
            return row
        except Exception as exp:
            print("update_data_row sheet_id:" + str(sheet_id) + " page_number:" + str(page_number) + " elemento:" + str(
                elemento) + " row_id:" + str(row_id))
            print(exp)
            traceback.print_exc()
            self.logging.error(
                "update_data_row sheet_id:" + str(sheet_id) + " page_number:" + str(page_number) + " elemento:" + str(
                    elemento) + " row_id:" + str(row_id))
            self.logging.error(exp)
            traceback.print_exc()

    def delete_multiple_data_row(self, sheet_id, page_number, row_ids: [], advanced_debug=False):
        try:
            row_ids.sort()
            sheet_id = self.load_sheet(sheet_id, advanced_debug=advanced_debug)
            page_number = self.select_page(sheet_id, page_number, advanced_debug=advanced_debug)
            for i in range(0, len(row_ids)):
                self.delete_data_row(sheet_id, page_number, row_ids[i] - i, advanced_debug=advanced_debug)
        except gspread.exceptions.APIError as exp:
            traceback.print_exc()
            # print(exp.args[0]["code"])
            if exp.args[0]["code"] == 429:
                Utility().wait(self.wait_time)
                rows = self.delete_multiple_data_row(sheet_id, page_number, row_ids, advanced_debug)
            elif exp.args[0]["code"] == 401:
                # Utility().wait(self.wait_time)
                self.recreate_cert()
                rows = self.delete_multiple_data_row(sheet_id, page_number, row_ids, advanced_debug)
            else:
                self.logging.warning(exp.args[0])
                traceback.print_exc()
            return rows
        except Exception as exp:
            print(" delete_multiple_data_row")
            print(exp)
            self.logging.error("delete_multiple_data_row")
            self.logging.error(exp)
            traceback.print_exc()


class DictTools:
    def __init__(self, loggin_name="DictTools", log_file="./DictTools.log"):
        """
        classe para gerenciar o tipo dictionary de forma mais simples
        :param loggin_name: nome do log que foi definido para a classe,altere apenas em caso seja necessário criar multiplas insstancias da função
        :param log_file: nome do arquivo de log que foi definido para a classe,altere apenas em caso seja necessário criar multiplas insstancias da função
        """
        self.logging = loggingSystem(loggin_name, arquivo=log_file)

    def normalize_index(self, dictionary_array: [dict], index, advanced_debug=False):
        try:
            if type(index) == type(""):
                index = index
                if advanced_debug:
                    self.logging.debug("string: " + str(index))
            elif type(index) == type(1):
                index = list(dictionary_array[0].keys())[index - 1]
                if advanced_debug:
                    self.logging.debug("integer: " + str(index))
            else:
                raise Exception("invalid array type")
            return index
        except Exception as exp:
            print(" normalize_index")
            print(exp)
            self.logging.error("normalize_index")
            self.logging.error(exp)
            traceback.print_exc()
            return None

    def sorter(self, dictionary_array: [dict], orderBy, reverse=False, remove_empty=False, advanced_debug=False):
        def integer_treat(input):
            if input == "":
                retorno = 0
            else:
                retorno = int(input)
            return int(retorno)

        if advanced_debug:
            self.logging.debug("pre ordenacao")
            self.logging.debug(dictionary_array)
        orderBy = self.normalize_index(dictionary_array=dictionary_array, index=orderBy, advanced_debug=advanced_debug)
        if type(dictionary_array[0][orderBy]) == type(1):
            retorno = sorted(dictionary_array, key=lambda element: integer_treat(element[orderBy]),
                             reverse=reverse)
        elif type(dictionary_array[0][orderBy]) == type(""):
            retorno = sorted(dictionary_array, key=itemgetter(orderBy), reverse=reverse)
        if advanced_debug:
            self.logging.debug("pos ordenacao")
            self.logging.debug(retorno)
        if remove_empty:
            for i in retorno:
                if i[orderBy] == "" or i[orderBy] == None or i[orderBy] == 0:
                    print(i)
                    retorno.remove(i)
        return retorno

    def case_changer(self, dictionary_array: [dict], column: [], case="lower", advanced_debug=False):
        if advanced_debug:
            self.logging.debug("pre ordenacao")
            self.logging.debug(dictionary_array)

        newcolumn = []
        for i in column:
            newcolumn.append(self.normalize_index(dictionary_array, i, advanced_debug=advanced_debug))
        retorno = []
        try:
            for index in dictionary_array:
                for i in newcolumn:
                    if case == "lower":
                        index[i] = index[i].lower()
                    elif case == "upper":
                        index[i] = index[i].upper()
                    elif case == "capitalize":
                        index[i] = index[i].capitalize()
                    elif case == "title":
                        index[i] = index[i].title()
                    else:
                        raise Exception("invalid case type")
                retorno.append(index)
            if advanced_debug:
                self.logging.debug("pos ordenacao")
                self.logging.debug(retorno)
            return retorno
        except Exception as error:
            print(error.args)
            self.logging.error(error.args)
            traceback.print_exc()
            return None

    def dict_to_multi_d_array(self, dictionary_array: [dict], advanced_debug=False):
        arrays = []
        for key in list(dictionary_array[0].keys()):
            arrays.append([])
            for element in dictionary_array:
                arrays[-1].append(element[key])
        if advanced_debug:
            self.logging.debug("conversao de array finalizada")
        return arrays

    def dict_to_cood_array(self, dictionary_array: [dict], element_orientation="row", advanced_debug=False):
        """

        """
        twodarray = self.dict_to_multi_d_array(dictionary_array, advanced_debug=advanced_debug)
        if element_orientation == "row":
            processedArray = []
        elif element_orientation == "col":
            processedArray = {"x": [], "y": [], "value": []}
        for x in range(0, len(twodarray)):
            for y in range(0, len(twodarray[x])):
                if element_orientation == "row":
                    tmp = {"y": x, "x": y, "value": twodarray[x][y]}
                    processedArray.append(tmp)
                elif element_orientation == "col":
                    processedArray["y"].append(x)
                    processedArray["x"].append(y)
                    processedArray["value"].append(twodarray[x][y])
                if advanced_debug:
                    self.logging.debug("x=" + str(y) + " y=" + str(x) + " values=" + str(twodarray[x][y]))
        if advanced_debug:
            self.logging.debug("conversao de array finalizada")
        return processedArray

    def compare_values(self, elementoA: {}, elementoB: {}):
        if elementoA.keys() == elementoB.keys():
            for i in elementoA.keys():
                if elementoA[i] != elementoB[i]:
                    return False
            return True
        else:
            return False

    def is_empty(self, elemento):
        for i in elemento:
            if i != 0 or i != "":
                return False
        return True


class Utility:
    def rewrite(self, text):
        sys.stdout.write("\r" + text)
        sys.stdout.flush()

    def wait(self, tempo, clean=True):
        espacador = ""
        for i in range(0, len(str(tempo))):
            espacador += " "
        for i in range(0, tempo + 1):
            self.rewrite("aguarde mais " + str(tempo - i) + " segundos para retornar o processo " + espacador)
            sleep(1)
        if clean:
            self.rewrite("")

    def clear_log_files(self, diretorio="./"):
        self.remove_all_files_of_type("log", diretorio)

    def remove_all_files_of_type(self, extension, diretorio="./"):
        arquivos = os.listdir(diretorio)
        for arquivo in arquivos:
            if arquivo.endswith("." + extension):
                os.remove(os.path.join(diretorio, arquivo))
    # exemplo funcional em linux de substituição de multiplas linhas
    # with output(output_type='dict') as output_lines:
    #     for i in range(10):
    #         output_lines['Moving file'] = "File_{}".format(i)
    #         for progress in range(100):
    #             output_lines['Total Progress'] = "[{done}{padding}] {percent}%".format(
    #                 done="#" * int(progress / 10),
    #                 padding=" " * (10 - int(progress / 10)),
    #                 percent=progress
    #             )
    #             sleep(0.05)

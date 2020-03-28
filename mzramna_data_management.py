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
        """
        classe para gerenciar arquivos
        :param loggin_name: nome do log que foi definido para a classe,altere apenas em caso seja necessário criar multiplas insstancias da função
        :param log_file: nome do arquivo de log que foi definido para a classe,altere apenas em caso seja necessário criar multiplas insstancias da função
        """
        self.logging = loggingSystem(loggin_name, filename=log_file)

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
        :return:
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

    def saveFileDictArray(self, arrayToSave, arquivo, advanced_debug=False):
        """

        :param arrayToSave: array de tipo dictionary para ser salvo no arquivo
        :param arquivo: nome do arquivo a ser acessado
        :param advanced_debug: ativa o sistema de logging se definido para True
        :return:
        """
        titulos = arrayToSave[0].keys()
        print(titulos)
        csvWrite = csv.DictWriter(open(arquivo, mode='a+'), fieldnames=titulos, delimiter=";")
        if advanced_debug:
            self.logging.debug("salvo dado no arquivo:" + str(arquivo))
        if os.stat(arquivo).st_size == 0:
            csvWrite.writeheader()
        for i in range(0, len(arrayToSave)):
            self.saveFileDict(arrayToSave[i], arquivo)

    def saveFileDict(self, elemento: dict, arquivo, advanced_debug=False):
        """

        :param elemento: elemento de tipo dictionary para ser salvo ao arquivo
        :param arquivo: nome do arquivo a ser acessado
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

        :param arquivo: nome do arquivo a ser acessado
        :param advanced_debug: ativa o sistema de logging se definido para True
        :return:
        """
        retorno = []
        with open(arquivo, mode='r') as file:
            if advanced_debug:
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

    def readFileDict(self, arquivo, line, advanced_debug=False):
        """

        :param arquivo: nome do arquivo a ser acessado
        :param line: linha do arquivo a ser lida
        :param advanced_debug: ativa o sistema de logging se definido para True
        :return:
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


class loggingSystem:
    def __init__(self, name, arquivo='arquivo.log', format='%(name)s - %(levelname)s - %(message)s',
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

        logger = logging.getLogger(name)
        logger.setLevel(level)
        logger.addHandler(handler)
        self.debug=logger.debug


class MYmysql:

    def __init__(self, user, passwd, host, database, loggin_name="mysql", log_file="mysql.log"):
        """
        classe para gerenciar banco de dados mysql
        :param user: usuario mysql para acessar o banco de dados
        :param passwd: senha de acesso ao banco de dados
        :param host: endereço de acesso ao banco de dados
        :param database: nome do banco de dados a ser acessado
        :param loggin_name: nome do log que foi definido para a classe,altere apenas em caso seja necessário criar multiplas insstancias da função
        :param log_file: nome do arquivo de log que foi definido para a classe,altere apenas em caso seja necessário criar multiplas insstancias da função
        """
        self.logging = loggingSystem(loggin_name, filename=log_file)

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


class MYntp():
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


class MYG_Sheets():
    def __init__(self, json_file, loggin_name="googleSheets", log_file="googleSheets.log"):
        """
        classe para gerenciar arquivos google sheets
        :param loggin_name: nome do log que foi definido para a classe,altere apenas em caso seja necessário criar multiplas insstancias da função
        :param log_file: nome do arquivo de log que foi definido para a classe,altere apenas em caso seja necessário criar multiplas insstancias da função
        """
        self.logging = loggingSystem(loggin_name, arquivo=log_file)
        self.creds = ServiceAccountCredentials.from_json_keyfile_name(json_file,
                                                                      ["https://spreadsheets.google.com/feeds",
                                                                       "https://www.googleapis.com/auth/spreadsheets",
                                                                       "https://www.googleapis.com/auth/drive.file",
                                                                       "https://www.googleapis.com/auth/drive"])
        self.client = gspread.authorize(self.creds)

    def add_reader_sheet(self, sheet_id, email, advanced_debug=False):
        """

        :param sheet_id: id da planilha google sheets
        :param email:
        :param advanced_debug: ativa o sistema de logging se definido para True
        :return:
        """
        self.client.insert_permission(sheet_id, email, perm_type="user", role="reader")
        if advanced_debug:
            self.logging.debug("usuario " + str(email) + " adicionada nova pessoa com permissao de leitura")

    def add_writer_sheet(self, sheet_id, email, advanced_debug=False):
        """

        :param sheet_id: id da planilha google sheets
        :param email:
        :param advanced_debug: ativa o sistema de logging se definido para True
        :return:
        """
        self.client.insert_permission(sheet_id, email, perm_type="user", role="writer")
        if advanced_debug:
            self.logging.debug("usuario " + str(email) + " adicionada nova pessoa com permissao de escrita")

    def change_owner_sheet(self, sheet_id, email, advanced_debug=False):
        """

        :param sheet_id: id da planilha google sheets
        :param email:
        :param advanced_debug: ativa o sistema de logging se definido para True
        :return:
        """
        self.client.insert_permission(sheet_id, email, perm_type="user", role="owner")
        if advanced_debug:
            self.logging.debug("arquivo teve propriedade movida para o usuario " + str(email))

    def change_sheet_to_public_read(self, sheet_id, advanced_debug=False):
        """

        :param sheet_id: id da planilha google sheets
        :param advanced_debug: ativa o sistema de logging se definido para True
        :return:
        """
        self.client.insert_permission(sheet_id,value=None, perm_type="anyone", role="reader")
        if advanced_debug:
            self.logging.debug("a planilha com id " + str(sheet_id) + " teve permissão de escrita publica habilitada")

    def change_sheet_to_public_write(self, sheet_id, advanced_debug=False):
        """

        :param sheet_id: id da planilha google sheets
        :param advanced_debug: ativa o sistema de logging se definido para True
        :return:
        """
        self.client.insert_permission(sheet_id,value=None, perm_type="anyone", role="writer")
        if advanced_debug:
            self.logging.debug("a planilha com id " + str(sheet_id) + " teve permissão de leitura publica habilitada")

    def retrive_data(self, sheet_id, page_number, import_range="all", advanced_debug=False):
        """

        :param sheet_id: id da planilha google sheets
        :param page_number: pagina da planilha a ser editada
        :param import_range:
        :param advanced_debug: ativa o sistema de logging se definido para True
        :return:
        """
        sheet = self.client.open_by_key(sheet_id)
        if import_range == "all":
            if advanced_debug:
                self.logging.debug("feita consulta na planilha: " + str(sheet_id) + " na página de nome " + str(
                    sheet.get_worksheet(page_number).title) + " e numero " + str(
                    sheet.get_worksheet(page_number).id) + " onde todos os dados foram retornados")
            return sheet.get_worksheet(page_number).get_all_records()
        else:
            if advanced_debug:
                self.logging.debug("feita consulta na planilha: " + str(sheet_id) + " na página de nome " + str(
                    sheet.get_worksheet(page_number).title) + " e numero " + str(
                    sheet.get_worksheet(page_number).id) + " onde foram retornados os valores do intervalo: " + str(
                    import_range))
            return sheet.get_worksheet(page_number).range(import_range)

    def create_sheet(self, sheet_name, owner=None, public_read=False, public_write=False, advanced_debug=False):
        """

        :param sheet_name: nome da planilha google sheets
        :param owner:
        :param public_read:
        :param public_write:
        :param advanced_debug: ativa o sistema de logging se definido para True
        :return: o id da nova coluna
        """
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

    def delete_sheet(self, sheet_id, advanced_debug=False):
        """

        :param sheet_id: id da planilha google sheets
        :param advanced_debug: ativa o sistema de logging se definido para True
        :return:
        """
        name = self.client.open_by_key(sheet_id).title
        self.client.del_spreadsheet(sheet_id)
        if advanced_debug:
            self.logging.debug("a planilha com id " + str(sheet_id) + " e nome de " + str(name) + " foi deletada")

    def update_data_range(self, sheet_id, page_number, list_of_row, list_of_col, list_of_values, advanced_debug=False):
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
        sheet = self.client.open_by_key(sheet_id)
        try:
            if len(list_of_col) != len(list_of_row) != len(list_of_values):
                raise Exception("invalid amount of elemnts on lists")
            else:
                cells = []
                for i in range(0, list_of_values):
                    cells.append(Cell(row=list_of_row[i], col=list_of_col[i], value=list_of_values[i]))
                sheet.get_worksheet(page_number).update_cells(cells)
                if advanced_debug:
                    self.logging.debug("foi concluida a inserção de dados na região definida")
        except Exception as exp:
            print(exp.args)
            self.logging.warning(exp.args)

    def update_data_cell(self, sheet_id, page_number, cell_cood, new_value, advanced_debug=False):
        """

        :param sheet_id: id da planilha google sheets
        :param page_number: pagina da planilha a ser editada
        :param cell_cood: coordenada da celula a ser manipulada,podendo ser coordenadas [x,y] em formato de array 1d,ou string padrão de excell "A1"
        :param new_value: novo valor a ser inserido
        :param advanced_debug: ativa o sistema de logging se definido para True
        :return:
        """
        sheet = self.client.open_by_key(sheet_id)
        try:
            if isinstance(cell_cood, str):
                sheet.get_worksheet(page_number).update_acell(cell_cood, new_value)
                if advanced_debug:
                    self.logging.debug("o intervalo da planilha foi atualizado")
            elif type(cell_cood) == type([]) and len(cell_cood) == 2:
                sheet.get_worksheet(page_number).update_cell(cell_cood[0], cell_cood[1], new_value)
                if advanced_debug:
                    self.logging.debug("o intervalo da planilha foi atualizado")
            else:
                raise Exception('invalid input cell_cood')
        except Exception as error:
            print(error.args)
            self.logging.warning(error.args)

    def delete_data_cell(self, sheet_id, page_number, cell_cood, advanced_debug=False):
        """

        :param sheet_id: id da planilha google sheets
        :param page_number: pagina da planilha a ser editada
        :param cell_cood: coordenada da celula a ser manipulada,podendo ser coordenadas [x,y] em formato de array 1d,ou string padrão de excell "A1"
        :param advanced_debug: ativa o sistema de logging se definido para True
        :return:
        """
        sheet = self.client.open_by_key(sheet_id)
        try:
            if isinstance(cell_cood, str):
                sheet.get_worksheet(page_number).update_acell(cell_cood, "")
                if advanced_debug:
                    self.logging.debug("a celula de endereco " + str(cell_cood) + " foi limpa")
            elif type(cell_cood) == type([]) and len(cell_cood) == 2:
                sheet.get_worksheet(page_number).update_cell(cell_cood[0], cell_cood[1], "")
                if advanced_debug:
                    self.logging.debug(
                        "a celula de coordenada [" + str(cell_cood[0]) + "," + str(cell_cood[1]) + "] foi limpa")
            else:
                raise Exception('invalid input cell_cood', cell_cood)
        except Exception as error:
            print(error.args)
            self.logging.warning(error.args)

    def add_data_row(self, sheet_id, page_number, elemento: dict, row_id=None, advanced_debug=False):
        """

        :param sheet_id: id da planilha google sheets
        :param page_number: pagina da planilha a ser editada
        :param elemento: dictionary com os elementos a serem inseridos
        :param row_id: numero da linha a ser inserida,se em branco apenas insere ao final do arquivo
        :param advanced_debug: ativa o sistema de logging se definido para True
        :return:
        """
        sheet = self.client.open_by_key(sheet_id)
        headers = sheet.get_worksheet(page_number).row_values(1)
        reordened = []
        try:
            if headers != []:
                for element in elemento:
                    if element not in headers:
                        raise Exception('header incompatible')
            else:
                sheet.get_worksheet(page_number).append_row(elemento.keys)
        except Exception as error:
            print(error.args)
            self.logging.warning(error.args)

        for header in headers:
            reordened.append(elemento[header])
        if row_id != None:
            sheet.get_worksheet(page_number).insert_row(reordened, index=row_id)
            if advanced_debug:
                self.logging.debug("a linha de numero " + str(row_id) + " foi adicionada")
        else:
            sheet.get_worksheet(page_number).append_row(reordened)
            if advanced_debug:
                self.logging.debug("a linha foi adicionada ao final do arquivo")

    def delete_data_row(self, sheet_id, page_number, row_id, advanced_debug=False):
        """

        :param sheet_id: id da planilha google sheets
        :param page_number: pagina da planilha a ser editada
        :param row_id: numero da linha a ser manipulada
        :param advanced_debug: ativa o sistema de logging se definido para True
        :return:
        """
        sheet = self.client.open_by_key(sheet_id)
        sheet.get_worksheet(page_number).delete_row(row_id)
        if advanced_debug:
            self.logging.debug("a linha de numero " + str(row_id) + " foi apagada")

    def add_page(self, sheet_id, page_name, minimum_col=24, minimum_row=10, advanced_debug=False):
        """

        :param sheet_id: id da planilha google sheets
        :param page_name: nome da nova pagina da planilha
        :param minimum_col:
        :param minimum_row:
        :param advanced_debug: ativa o sistema de logging se definido para True
        :return: id da nova página criada
        """
        sheet = self.client.open_by_key(sheet_id)
        id = sheet.add_worksheet(title=page_name, rows=minimum_row, cols=minimum_col).id
        if advanced_debug:
            self.logging.debug("foi criada uma página nova com titulo " + str(page_name) + " seu id é " + str(id))
        return id

    def delete_page(self, sheet_id, page_number, advanced_debug=False):
        """

        :param sheet_id: id da planilha google sheets
        :param page_number: pagina da planilha a ser editada numero da página da planilha
        :param advanced_debug: ativa o sistema de logging se definido para True
        :return:
        """
        sheet = self.client.open_by_key(sheet_id)
        name = self.client.open_by_key(sheet_id).get_worksheet(page_number).title
        sheet.del_worksheet(page_number)
        if advanced_debug:
            self.logging.debug("pagina numero " + str(page_number) + "com nome " + str(name) + " foi deletada")

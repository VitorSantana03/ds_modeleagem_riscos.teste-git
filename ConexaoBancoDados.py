from trino.auth  import BasicAuthentication
from trino.dbapi import connect
from credenciais import credenciais

import sqlalchemy as sa
import pandas as pd
import pyodbc
import urllib

class ConexaoBancoDados:
    """
    Essa classe tem o intuito de fazer conexão tanto com o SQL SERVER e o LAKE.
    Temos apenas que instanciala, é sugerido usar
    from ConexaoBancoDados import ConexaoBancoDados as CDB
    cdb = CDB()
    Podemos depois usar por exemplo para o trino
    cdb.realizaConsulta(query,consultaTrino=1)
    Ou podemos usar para o sql
    cdb.realizaConsulta(query,consultaTrino=0)
    Além de ter outros recursos relaciondos aos bancos de dados
    """
    stringConn ="""DRIVER={ODBC Driver 17 for SQL Server};
                           SERVER=VAWPDBRISCOT01; DATABASE=RiscoDB;Trusted_Connection=yes"""
    params = urllib.parse.quote_plus(stringConn)
    engine = sa.create_engine("mssql+pyodbc:///?odbc_connect={}".format(params), fast_executemany=True)
    
    def conexaoSql(self):
        """
        Configuração de conexão com o Sql 
        """
        sqlConn = (pyodbc.connect(self.stringConn))
        cursor = sqlConn.cursor()
        return cursor,sqlConn

    def conexaoTrino(self):
        """
        Configuração de conexão com o Lake
        """
        return  connect(
                        user        = credenciais['USUARIO'],
                        auth        = BasicAuthentication(credenciais['USUARIO'], credenciais['SENHA']),
                        http_scheme = "https",
                        host        = 'trino.de.in.devneon.com.br',
                        catalog     ='hive',
                        port        = 443
                        )
        
    def atualizaTabela(self,queryAtt,trino=0):
        """
        Realizar um append em uma tabela usando uma query no seguinte formato:
        INSERTO INTO
        SELECT * FROM TABELA
        """
        if trino :
            cursor = self.conexaoTrino().cursor()
            cursor.execute(queryAtt)
            cursor.fetchall()
            cursor.close()
        else:
            cursor,sqlConn = self.conexaoSql()
            cursor.execute(queryAtt)
            cursor.commit()
            sqlConn.commit()
            sqlConn.close()
    
    def deletardaTabelaSql(self,queryDelete):
        """
        Passe uma query com o formato a seguir, se deseja excluir uma parte da Tabela
            DELETE FROM Tabela WHERE Condicao
        Caso deseja excluir toda a tabela passe uma query com o seguinte formato
            DROP TABLE Tabela
        """
        cursor,sqlConn = self.conexaoSql()
        cursor.execute(queryDelete)
        cursor.commit()
        sqlConn.commit()
        sqlConn.close()
    
    def realizaConsulta(self,query,trino=0) ->pd.DataFrame:
        """
        Podemos realizar consultas pelo lake selecionando o trino = 1 e passando a query desejada 
        como mostrado abaixo:
            realizaConsulta(query,trino=1)
        Ou podemos realizar uma consulta pelo Sql selecionando o trino = 0 e passando a query desejada 
        como mostrado abaixo:
            realizaConsulta(query)
        """
        if trino:
            df = pd.read_sql_query(query,self.conexaoTrino())
        else:
            cursor,sqlConn = self.conexaoSql()
            df = pd.read_sql_query(query,sqlConn)
            sqlConn.close()
        return df

    def atualizaTabelaDataFrame(self,df:pd.DataFrame,configSql:dict):
        """
        Para atualizar uma Tabela no Sql,passe um DataFrame com os dados e um dicionario
        com configuração sendo elas por exemplo:
            {
                'Tabela':'TabelaSubirSql'
                'Schema':'NEON\/002502'
            }
        """
        df.to_sql(configSql['Tabela'],
                    con=self.engine,
                    index=False,
                    if_exists='append',
                    schema=configSql['Schema'])
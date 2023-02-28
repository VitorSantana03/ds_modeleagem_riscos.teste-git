## Classe: ConexaoBancoDados
# 
Para utilizarmos essa classe temos que primeiro instalar os pacotes presente no arquivo `requirements.txt`, com o comando: 
```python
!pip install requeriments.txt
```
Após efetuar esse procedimento iremos criar um arquivo chamado `credenciais.py` na pasta 📁onde você salvou a classe `ConexaoBancoDados.py`. Esse arquivo tem que ter o seguinte formato com seus acessos:

```python
credenciais = {
    'USUARIO':'u00xxxx',
    'SENHA'  :'xxxxxxx'
}
```
Após efetuar esse processo vc terá que instanciar a classe para utilizar seus métodos, sugerimos usar da seguinte forma:

```python
from ConexaoBancoDados import ConexaoBancoDados as CDB
cdb = CDB()
```
Para efetuar uma consulta de uma base no `SQL` podemos efetuar da seguinte forma:

```python
query = r"""select top 10 *
            from 
            RiscoDB.CREDITO.CreditoContrato 
        """
dadosSql = cdb.realizaConsulta(query,trino=0)
dadosSql.head()
```
Caso deseje realizar uma consulta no `Trino` opte por utilizar o comando da seguinte forma

```python
query = r"""select * 
            from 
                neondw_bi.staging_bi_creditocontratocliente 
            limit 10
        """
dadosTrino = cdb.realizaConsulta(query,trino=1)
```
Pode ser visualizado os exemplos destacados acima no notebook `utilizandoClasse.ipynb`.

**Obeservação Importante :**
Não de commit de suas credencias juntamente com seu código na hora de subir em um repositorio, para isso crie um arquivo `.gitignore`, com o seguinte formato:
```python
# Ignore a pasta __pycache__
__pycache__
# Ignore o arquivo credenciais.py
credenciais.py

```
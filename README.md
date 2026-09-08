# MVP: Pipeline de Dados de Bilheteria de Cinema

**Plataforma Utilizada:** Databricks (Lakehouse Architecture) / Lakeview Dashboards.

**Objetivo:** 
O presente MVP (Produto Mínimo Viável) tem como objetivo construir um pipeline de dados analítico de ponta a ponta na nuvem. O foco é processar dados brutos do mercado cinematográfico brasileiro, cruzá-los com metadados de filmes, e estruturá-los para responder a perguntas estratégicas de negócio sobre faturamento, sazonalidade e comportamento do público, culminando em um painel de inteligência de mercado executivo.

<img width="1024" height="454" alt="image" src="https://github.com/user-attachments/assets/d0e227a7-b5f5-41e6-9632-e9c82c2acb63" />

---

## Sumário
* [1. Contexto de Negócios e Perguntas](#1-contexto-de-negócios-e-perguntas-etapas-2-e-41)
* [2. Coleta e Carga dos Dados](#2-coleta-e-carga-dos-dados-etapa-42)
* [3. Modelagem e Catálogo de Dados](#3-modelagem-e-catálogo-de-dados-etapa-43)
* [4. Pipeline de Dados](#4-pipeline-de-dados-etapa-44)
* [5. Qualidade de Dados](#5-qualidade-de-dados-etapa-45)
* [6. Análise de Dados](#6-análise-de-dados-etapa-45)
* [7. Autoavaliação](#7-autoavaliação)

### 1. Contexto de Negócios e Perguntas (Etapas 2 e 4.1)

**Contexto de Negócio:**
O mercado de exibição cinematográfica precisa entender onde estão seus gargalos operacionais e onde a receita é efetivamente gerada. Este projeto analisa dados de bilheteria e exibições no Brasil, cruzando o volume de público com metadados dos filmes. 

**Estrutura dos Dados Brutos e Licença:** A fonte primária e majoritária dos dados é a Agência Nacional do Cinema (Ancine), sob a licença de Dados Abertos do Governo Federal. Para o enriquecimento do catálogo, utilizou-se a API do TMDB (The Movie Database) para metadados gerais de produção, e a API do OMDb (The Open Movie Database) para a extração de notas e consensos da crítica especializada (Metacritic e Rotten Tomatoes). Ambas as APIs permitem o uso não comercial para fins acadêmicos.

**Perguntas de Negócio (Mapeadas para os Dashboards):**
1. **Top 10 filmes com maiores públicos:** Quais são os 10 filmes que atraíram as maiores audiências no Brasil?
2. **Ano Atual:** Qual é o desempenho financeiro e de público mês a mês no ano corrente?
3. **Faturamento x Ano:** Qual é a evolução histórica do faturamento anual do setor?
4. **Bilheteria entre os meses (2021 a 2026):** Como a sazonalidade afeta o desempenho das salas ao longo dos anos?
5. **Dias da Semana que mais vendem:** Quais dias da semana concentram o maior volume de vendas de ingressos?
6. **Distribuição Geográfica:** Como o público consumidor de cinema está distribuído entre os estados brasileiros (UF)?
7. **Top 10 Gêneros:** Quais são os 10 gêneros cinematográficos mais rentáveis e populares?

## 2. Coleta e Carga dos Dados (Etapa 4.2)

A ingestão de dados envolveu um trabalho prévio de *Data Wrangling* para garantir a viabilidade analítica antes da subida para o Databricks:

<img width="1768" height="334" alt="image" src="https://github.com/user-attachments/assets/a314bea8-c7c2-4601-afac-9cf1699f06f4" />

1. **Extração Primária:** Download da base histórica completa de bilheteria pública da Ancine (de 2014 até o presente).
2. **Filtro Temporal e Limpeza:** Para manter a relevância do mercado recente, os dados anteriores a 2021 foram excluídos, estabelecendo um recorte de 5 anos de bilheteria. Colunas sem valor analítico foram removidas para otimizar o processamento.
3. **Consolidação:** Os arquivos foram concatenados em um único arquivo CSV e passaram por correção de *encoding* para UTF-8, evitando erros em caracteres especiais dos títulos.
4. **Enriquecimento via API:** Foi construído um script de conexão com a API do TMDB para buscar metadados específicos de cada obra, incorporando ao dataset os seguintes campos: `TITULO_BRASIL`, `TMDB_ID`, `POPULARIDADE`, `NOTA_MEDIA`, `VOTOS`, `DATA_LANCAMENTO`, `ORCAMENTO_USD` e `GENEROS`. Em paralelo, consumiu-se a API do OMDb para extrair e integrar os índices de avaliação do Metacritic e Rotten Tomatoes.

**Execução do Pré-processamento**
Para automatizar as etapas de limpeza (Itens 2 e 3) e evitar gargalos de memória, foi desenvolvido o script [`src/JuntarArquivosCSV.py`](src/JuntarArquivosCSV.py). Ele atua diretamente na máquina local aplicando os filtros temporais, removendo linhas com público nulo, forçando a codificação `UTF-8` e aplicando uma regra de negócio de corte: manter apenas filmes com público acumulado superior a 5.000 espectadores. 

Após esse tratamento inicial, o CSV consolidado e os dados das APIs foram carregados diretamente no DBFS/Volumes do Databricks, representando a camada Bronze (*landing zone*).

<br>

**Scripts Desenvolvidos nesta Etapa:**

<details>
<summary><b>🐍 Ver o código de Pré-processamento e Data Wrangling (JuntarArquivosCSV.py)</b></summary>
<br>
Este script foi utilizado localmente antes da ingestão para otimizar o processamento na nuvem. Ele consolida os arquivos, filtra os anos de 2021 a 2026, remove dados com público nulo, aplica a regra de negócio (público > 5.000) e garante a codificação UTF-8.

```python
import pandas as pd
import logging
from pathlib import Path
from typing import List

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def consolidate_ancine_data(source_directory: str, target_years: List[str], output_filepath: str) -> None:
    
    source_path = Path(source_directory)
    if not source_path.exists():
        logging.error(f"Diretório não encontrado: {source_directory}")
        return

    all_csv_files = list(source_path.glob("*.csv"))
    filtered_files = [
        f for f in all_csv_files 
        if any(year in f.name for year in target_years) and "dados_brutos" not in f.name
    ]
    
    if not filtered_files:
        logging.warning("Nenhum arquivo correspondente aos anos alvo foi encontrado.")
        return
        
    logging.info(f"Foram encontrados {len(filtered_files)} arquivos para processamento.")

    target_columns = [
        'DATA_EXIBICAO', 
        'TITULO_ORIGINAL', 
        'TITULO_BRASIL', 
        'PAIS_OBRA', 
        'PUBLICO', 
        'RAZAO_SOCIAL_DISTRIBUIDORA',
        'NOME_SALA',
        'MUNICIPIO_SALA_COMPLEXO',
        'UF_SALA_COMPLEXO',
    ]

    dataframes_list = []

    for filepath in filtered_files:
        logging.info(f"Lendo arquivo: {filepath.name}")
        try:
            df_temp = pd.read_csv(filepath, sep=';', encoding='utf-8', dtype=str, usecols=target_columns)
            dataframes_list.append(df_temp)
        except Exception as e:
            logging.error(f"Falha ao processar {filepath.name}. Erro: {e}")

    if dataframes_list:
        logging.info("Concatenando os dados...")
        df_raw = pd.concat(dataframes_list, ignore_index=True)
        
        df_raw = df_raw.dropna(subset=['PUBLICO'])
        df_raw['PUBLICO'] = pd.to_numeric(df_raw['PUBLICO'], errors='coerce')
        df_raw = df_raw.dropna(subset=['PUBLICO'])
        df_raw['PUBLICO'] = df_raw['PUBLICO'].astype(int)
        
        logging.info("Calculando o público total por filme...")
        total_por_filme = df_raw.groupby('TITULO_ORIGINAL')['PUBLICO'].sum().reset_index()
        filmes_sucesso = total_por_filme[total_por_filme['PUBLICO'] >= 5000]['TITULO_ORIGINAL']
        
        df_raw = df_raw[df_raw['TITULO_ORIGINAL'].isin(filmes_sucesso)]
        
        logging.info("Agrupando os dados e consolidando a bilheteria nacionalmente...")
        df_agrupado = df_raw.groupby([
            'DATA_EXIBICAO', 
            'TITULO_ORIGINAL', 
            'TITULO_BRASIL', 
            'PAIS_OBRA', 
            'RAZAO_SOCIAL_DISTRIBUIDORA',
            'NOME_SALA',
            'MUNICIPIO_SALA_COMPLEXO',
            'UF_SALA_COMPLEXO',
        ])['PUBLICO'].sum().reset_index()
        
        logging.info("Exportando dados consolidados...")
        df_agrupado.to_csv(output_filepath, index=False, sep=';', encoding='utf-8-sig')
        
        logging.info(f"Processo finalizado com sucesso! Arquivo gerado com apenas {df_agrupado.shape[0]} linhas.")

if __name__ == "__main__":
    SOURCE_DIR = r"C:\Users\Daniel Siqueira\Documents\Puc"
    TARGET_YEARS = ['2021', '2022', '2023', '2024', '2025', '2026']
    OUTPUT_FILE = r"C:\Users\Daniel Siqueira\Documents\Puc\ancine_dados_brutos_2021_2026.csv"
    
    consolidate_ancine_data(
        source_directory=SOURCE_DIR,
        target_years=TARGET_YEARS,
        output_filepath=OUTPUT_FILE
    )


```
</details>

<details>
<summary><b>🐍 Ver o código de Tratamento de Encoding (PadronizacaoUTF8.py)</b></summary>
<br>
Além da consolidação, foi desenvolvido um script preparatório focado exclusivamente na limpeza e padronização do <i>encoding</i>. Essa etapa garante a integridade dos caracteres especiais da língua portuguesa (como cedilhas e acentos) antes da carga na nuvem, assegurando a qualidade do dado bruto para consumo no Data Lakehouse.

```python
import pandas as pd
import logging
from pathlib import Path

# Configuração do log para padrão corporativo
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def enforce_utf8_encoding(input_filepath: str, output_filepath: str) -> None:
    """
    Lê a base de Dados Brutos e padroniza o encoding de todo o arquivo para UTF-8.
    Isso previne erros de leitura de caracteres especiais brasileiros (ç, acentos) 
    durante a etapa de Carga no Databricks.
    """
    input_path = Path(input_filepath)
    if not input_path.exists():
        logging.error(f"Ihh, arquivo não encontrado: {input_filepath}")
        return
        
    logging.info(f"Iniciando leitura do arquivo bruto: {input_path.name}...")
    try:
        # Lê o arquivo forçando o tipo string para não quebrar nenhuma formatação
        df = pd.read_csv(input_path, sep=';', dtype=str, encoding='utf-8')
        
        logging.info("Aplicando padronização UTF-8 em todas as linhas...")
        
        # O pulo do gato: salvar como 'utf-8-sig' força o arquivo a ter a assinatura (BOM),
        # garantindo que ferramentas como Excel e Databricks reconheçam os acentos de primeira.
        df.to_csv(output_filepath, sep=';', index=False, encoding='utf-8-sig')
        
        logging.info(f"Sucesso total! Arquivo padronizado e salvo como: {output_filepath}")
        logging.info(f"Total de registros processados e formatados: {df.shape[0]}")
        
        except Exception as e:
        logging.error(f"Deu xabu na conversão. Detalhes do erro: {e}")

if __name__ == "__main__":
    # Caminhos apontando direto pra tua pasta do projeto
    ARQUIVO_ENTRADA = r"C:\Users\Daniel Siqueira\Documents\Puc\ancine_dados_brutos_2021_2026.csv"
    ARQUIVO_SAIDA = r"C:\Users\Daniel Siqueira\Documents\Puc\ancine_dados_brutos_utf8_2021_2026.csv"
    
    enforce_utf8_encoding(ARQUIVO_ENTRADA, ARQUIVO_SAIDA)
```
</details>

<details>
<summary><b>🐍 Ver o código de Ingestão da Camada Bronze (PySpark)</b></summary>
<br>
Após o pré-processamento local e a subida para os <i>Volumes</i> do Databricks, este script PySpark realiza a leitura do CSV garantindo a codificação UTF-8 e salva os dados no formato nativo Delta, consolidando oficialmente a tabela na camada Bronze da arquitetura Medallion.

```python
df_ancine_1gb = spark.read.format("csv") \
    .option("header", "true") \
    .option("sep", ";") \
    .option("encoding", "UTF-8") \
    .load("/Volumes/dbacademy/default/puc/ancine_dados_brutos_2021_2026.csv")

# Recriando a tabela Bronze limpa com a acentuação correta
df_ancine_1gb.write.format("delta").mode("overwrite").saveAsTable("bronze_ancine")
print("✅ Base Bronze recriada com UTF-8: Adeus, texto quebrado!")
```
</details>

### 3. Modelagem e Catálogo de Dados (Etapa 4.3)

A modelagem adotada segue o princípio do *Lakehouse*, utilizando uma abordagem de *Star Schema* adaptada para o Delta Lake:
*   **dim_filme (Dimensão - Silver):** Tabela contendo atributos descritivos extraídos da API do TMDB.
*   **gold_features_bilheteria (Fato - Gold):** Tabela central agregando métricas de negócio e chaves temporais da Ancine.

**Catálogo de Dados (Resumo das principais métricas):**
*   **`dim_filme`**: `tmdb_id` (Integer - PK), `titulo_brasil` (String), `classificacao_etaria` (String), `nota_media` (Decimal), `votos` (Integer), `popularidade` (Decimal), `orcamento_usd` (Decimal), `generos` (String), `data_lancamento` (Date).
*   **`gold_features_bilheteria`**: `tmdb_id` (Integer - FK), `data_exibicao` (Date), `dia_semana` (String), `publico` (Integer), `faturamento_estimado_r$` (Decimal - Métrica gerada multiplicando o público por um ticket médio de R$ 20,00).

<img width="1049" height="647" alt="image" src="https://github.com/user-attachments/assets/73832876-18d5-4f83-9052-c89d8f470797" />

### 4. Pipeline de Dados (Etapa 4.4)

O processo de ETL (Extração, Transformação e Carga) foi centralizado em *Notebooks* no Databricks, implementando a Arquitetura Medalhão:
1.  **Extract:** Leitura dos dados armazenados na camada Bronze.
2.  **Transform:** Na camada **Silver**, ocorreu a remoção de duplicatas, tipagem correta de colunas (datas e decimais) e tratamento de inconsistências. Na camada **Gold**, as tabelas foram unidas (`JOIN`) pelo `tmdb_id` e enriquecidas com agregações de negócio (cálculo de faturamento estimado e extração de dias da semana).
3.  **Load:** As tabelas finais foram salvas fisicamente em formato Delta (`.format("delta").saveAsTable(...)`), garantindo transações ACID e performance nativa.

<img width="1642" height="797" alt="image" src="https://github.com/user-attachments/assets/580b7551-ad5b-4f7d-a135-e593d6f10fec" />

<br>

<details>
<summary><b>🐍 Ver o script completo de Modelagem da Camada Silver (PySpark)</b></summary>
<br>
Na camada Silver, os dados brutos ingeridos passam por limpeza, tipagem e estruturação em um modelo multidimensional (Star Schema). Este script PySpark demonstra o processo de criação de todas as 12 tabelas do projeto (Dimensões, Fato de Exibições e Tabelas Associativas para relações N:N), garantindo a qualidade da informação e salvando os registros em formato Delta Lake nativo.

```python
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ==============================================================================
# 1. LEITURA DA CAMADA BRONZE
# ==============================================================================
df_bronze_ancine = spark.read.table("bronze_ancine")
df_bronze_tmdb = spark.read.table("bronze_tmdb")
# Caso as notas estejam em tabela separada ou embutidas na bronze_tmdb/bronze_omdb:
df_bronze_omdb = spark.read.table("bronze_omdb") if spark.catalog.tableExists("bronze_omdb") else None

# ==============================================================================
# 2. CRIAÇÃO DAS TABELAS DE DIMENSÃO (dim_*)
# ==============================================================================

# dim_filme
df_dim_filme = df_bronze_tmdb.select(
    F.col("tmdb_id").cast("int").alias("id_filme"),
    F.trim(F.col("titulo_original")).alias("titulo_original"),
    F.trim(F.col("titulo_brasil")).alias("titulo_brasil"),
    F.to_date(F.col("data_lancamento"), "yyyy-MM-dd").alias("data_lancamento"),
    F.year(F.to_date(F.col("data_lancamento"))).alias("ano_lancamento"),
    F.coalesce(F.trim(F.col("classificacao_etaria")), F.lit("Não Informada")).alias("classificacao_etaria"),
    F.col("orcamento_usd").cast("decimal(15,2)").alias("orcamento_usd"),
    F.col("popularidade").cast("decimal(10,3)").alias("popularidade")
).dropDuplicates(["id_filme"])

# dim_ator (Extração e desaninhamento do elenco)
df_atores_raw = df_bronze_tmdb.select(
    F.explode(F.split(F.col("atores"), ",\\s*")).alias("nome_ator")
).filter(F.col("nome_ator").isNotNull() & (F.trim(F.col("nome_ator")) != ""))

w_ator = Window.orderBy("nome_ator")
df_dim_ator = df_atores_raw.distinct() \
    .withColumn("id_ator", F.row_number().over(w_ator)) \
    .select("id_ator", F.trim(F.col("nome_ator")).alias("nome_ator"))

# dim_diretor
df_diretor_raw = df_bronze_tmdb.select(
    F.explode(F.split(F.col("diretores"), ",\\s*")).alias("nome_diretor")
).filter(F.col("nome_diretor").isNotNull() & (F.trim(F.col("nome_diretor")) != ""))

w_diretor = Window.orderBy("nome_diretor")
df_dim_diretor = df_diretor_raw.distinct() \
    .withColumn("id_diretor", F.row_number().over(w_diretor)) \
    .select("id_diretor", F.trim(F.col("nome_diretor")).alias("nome_diretor"))

# dim_genero
df_genero_raw = df_bronze_tmdb.select(
    F.explode(F.split(F.col("generos"), ",\\s*")).alias("nome_genero")
).filter(F.col("nome_genero").isNotNull() & (F.trim(F.col("nome_genero")) != ""))

w_genero = Window.orderBy("nome_genero")
df_dim_genero = df_genero_raw.distinct() \
    .withColumn("id_genero", F.row_number().over(w_genero)) \
    .select("id_genero", F.trim(F.col("nome_genero")).alias("nome_genero"))

# dim_produtora
df_produtora_raw = df_bronze_tmdb.select(
    F.explode(F.split(F.col("produtoras"), ",\\s*")).alias("nome_produtora")
).filter(F.col("nome_produtora").isNotNull() & (F.trim(F.col("nome_produtora")) != ""))

w_produtora = Window.orderBy("nome_produtora")
df_dim_produtora = df_produtora_raw.distinct() \
    .withColumn("id_produtora", F.row_number().over(w_produtora)) \
    .select("id_produtora", F.trim(F.col("nome_produtora")).alias("nome_produtora"))

# dim_localidade (Originada dos dados de exibição da Ancine)
w_localidade = Window.orderBy("uf", "municipio", "nome_sala")
df_dim_localidade = df_bronze_ancine.select(
    F.trim(F.col("UF_SALA_COMPLEXO")).alias("uf"),
    F.trim(F.col("MUNICIPIO_SALA_COMPLEXO")).alias("municipio"),
    F.trim(F.col("NOME_SALA")).alias("nome_sala"),
    F.trim(F.col("RAZAO_SOCIAL_DISTRIBUIDORA")).alias("distribuidora")
).distinct() \
 .withColumn("id_localidade", F.row_number().over(w_localidade))

# dim_avaliacoes (TMDB + OMDb / Metacritic / Rotten Tomatoes)
if df_bronze_omdb:
    df_dim_avaliacoes = df_bronze_tmdb.join(df_bronze_omdb, on="tmdb_id", how="left") \
        .select(
            F.col("tmdb_id").cast("int").alias("id_filme"),
            F.col("nota_media").cast("decimal(3,1)").alias("nota_tmdb"),
            F.col("votos").cast("int").alias("votos_tmdb"),
            F.col("imdb_rating").cast("decimal(3,1)").alias("nota_imdb"),
            F.col("metascore").cast("int").alias("nota_metacritic"),
            F.col("rotten_tomatoes_rating").alias("score_rotten_tomatoes")
        ).dropDuplicates(["id_filme"])
else:
    df_dim_avaliacoes = df_bronze_tmdb.select(
        F.col("tmdb_id").cast("int").alias("id_filme"),
        F.col("nota_media").cast("decimal(3,1)").alias("nota_tmdb"),
        F.col("votos").cast("int").alias("votos_tmdb"),
        F.col("popularidade").cast("decimal(10,3)").alias("popularidade")
    ).dropDuplicates(["id_filme"])


# ==============================================================================
# 3. TABELAS DE RELACIONAMENTO / ASSOCIATIVAS (filme_*)
# ==============================================================================

# filme_ator
df_filme_ator = df_bronze_tmdb.select(
    F.col("tmdb_id").cast("int").alias("id_filme"),
    F.explode(F.split(F.col("atores"), ",\\s*")).alias("nome_ator")
).join(df_dim_ator, on="nome_ator", how="inner") \
 .select("id_filme", "id_ator").distinct()

# filme_diretor
df_filme_diretor = df_bronze_tmdb.select(
    F.col("tmdb_id").cast("int").alias("id_filme"),
    F.explode(F.split(F.col("diretores"), ",\\s*")).alias("nome_diretor")
).join(df_dim_diretor, on="nome_diretor", how="inner") \
 .select("id_filme", "id_diretor").distinct()

# filme_genero
df_filme_genero = df_bronze_tmdb.select(
    F.col("tmdb_id").cast("int").alias("id_filme"),
    F.explode(F.split(F.col("generos"), ",\\s*")).alias("nome_genero")
).join(df_dim_genero, on="nome_genero", how="inner") \
 .select("id_filme", "id_genero").distinct()

# filme_produtora
df_filme_produtora = df_bronze_tmdb.select(
    F.col("tmdb_id").cast("int").alias("id_filme"),
    F.explode(F.split(F.col("produtoras"), ",\\s*")).alias("nome_produtora")
).join(df_dim_produtora, on="nome_produtora", how="inner") \
 .select("id_filme", "id_produtora").distinct()


# ==============================================================================
# 4. CRIAÇÃO DA TABELA FATO (fato_exibicao)
# ==============================================================================

df_fato_exibicao = df_bronze_ancine.select(
    F.col("tmdb_id").cast("int").alias("id_filme"),
    F.to_date(F.col("DATA_EXIBICAO"), "dd/MM/yyyy").alias("data_exibicao"),
    F.col("PUBLICO").cast("int").alias("publico"),
    F.trim(F.col("UF_SALA_COMPLEXO")).alias("uf"),
    F.trim(F.col("MUNICIPIO_SALA_COMPLEXO")).alias("municipio"),
    F.trim(F.col("NOME_SALA")).alias("nome_sala"),
    F.trim(F.col("RAZAO_SOCIAL_DISTRIBUIDORA")).alias("distribuidora")
).join(
    df_dim_localidade,
    on=["uf", "municipio", "nome_sala", "distribuidora"],
    how="left"
).select(
    F.col("id_filme"),
    F.col("id_localidade"),
    F.col("data_exibicao"),
    F.col("publico")
).filter(F.col("id_filme").isNotNull())


# ==============================================================================
# 5. CARGA DAS TABELAS EM DELTA LAKE (CAMADA SILVER)
# ==============================================================================

tabelas_silver = {
    "dim_ator": df_dim_ator,
    "dim_avaliacoes": df_dim_avaliacoes,
    "dim_diretor": df_dim_diretor,
    "dim_filme": df_dim_filme,
    "dim_genero": df_dim_genero,
    "dim_localidade": df_dim_localidade,
    "dim_produtora": df_dim_produtora,
    "fato_exibicao": df_fato_exibicao,
    "filme_ator": df_filme_ator,
    "filme_diretor": df_filme_diretor,
    "filme_genero": df_filme_genero,
    "filme_produtora": df_filme_produtora
}

for nome_tabela, df_tabela in tabelas_silver.items():
    df_tabela.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(nome_tabela)
    print(f"✅ Tabela Delta salva com sucesso: {nome_tabela}")
```
</details>

<details>
<summary><b>🐍 Ver o código de Agregação da Camada Gold (PySpark)</b></summary>
<br>
A camada Gold é o estágio final do nosso Lakehouse, focado na entrega de valor para o negócio. Este script realiza o cruzamento (JOIN) entre a tabela fato e as dimensões construídas na etapa Silver, aplicando regras de negócio para gerar novas métricas (como o `faturamento_estimado_r$` e `dia_semana_nome`). O resultado é uma tabela desnormalizada e otimizada para o consumo direto nos dashboards executivos e modelos de Machine Learning.

```python
from pyspark.sql import functions as F

# ==============================================================================
# 1. LEITURA DAS TABELAS MODELADAS (CAMADA SILVER)
# ==============================================================================
df_fato_exibicao = spark.read.table("fato_exibicao")
df_dim_filme = spark.read.table("dim_filme")
df_dim_localidade = spark.read.table("dim_localidade")

# ==============================================================================
# 2. CRUZAMENTO E APLICAÇÃO DE REGRAS DE NEGÓCIO (CAMADA GOLD)
# ==============================================================================
# O ticket médio definido para o MVP foi de R$ 20,00 por espectador.
TICKET_MEDIO = 20.00

df_gold_bilheteria = df_fato_exibicao.alias("fato") \
    .join(df_dim_filme.alias("filme"), F.col("fato.id_filme") == F.col("filme.id_filme"), "left") \
    .join(df_dim_localidade.alias("loc"), F.col("fato.id_localidade") == F.col("loc.id_localidade"), "left") \
    .select(
        # Chaves e Metadados do Filme
        F.col("fato.id_filme"),
        F.col("filme.titulo_brasil"),
        F.col("filme.classificacao_etaria"),
        F.col("filme.ano_lancamento"),
        
        # Localidade
        F.col("loc.uf"),
        F.col("loc.municipio"),
        F.col("loc.nome_sala"),
        
        # Temporal e Métricas Base
        F.col("fato.data_exibicao"),
        F.col("fato.publico"),
        
        # Regras de Negócio: Faturamento Estimado
        (F.col("fato.publico") * TICKET_MEDIO).cast("decimal(15,2)").alias("faturamento_estimado_r$"),
        
        # Regras de Negócio: Sazonalidade Diária
        F.dayofweek(F.col("fato.data_exibicao")).alias("dia_semana_id"),
        F.expr("""
            CASE dayofweek(fato.data_exibicao)
                WHEN 1 THEN 'Domingo' 
                WHEN 2 THEN 'Segunda-feira' 
                WHEN 3 THEN 'Terça-feira' 
                WHEN 4 THEN 'Quarta-feira' 
                WHEN 5 THEN 'Quinta-feira' 
                WHEN 6 THEN 'Sexta-feira' 
                WHEN 7 THEN 'Sábado' 
            END
        """).alias("dia_semana_nome")
    )

# ==============================================================================
# 3. CARGA DA TABELA FINAL EM DELTA LAKE (CAMADA GOLD)
# ==============================================================================
df_gold_bilheteria.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold_features_bilheteria")

print("✅ Camada Gold processada com sucesso: Tabela 'gold_features_bilheteria' pronta para os Dashboards!")
```
</details>

### 5. Qualidade de Dados (Etapa 4.5)

Durante a exploração dos dados, detectou-se uma falha de completude crítica: 146 registros cruciais (representando mais de 600 milhões de ingressos) apresentavam valor `NULL` na coluna de classificação etária na camada Silver, pois essa informação específica não vinha na rota principal da API do TMDB.

**Resolução:** Em vez de excluir os dados (o que corromperia o faturamento total da Ancine) ou inferir valores irreais, utilizou-se a função SQL `COALESCE` para categorizá-los como "Não Informada". Paralelamente, desenvolveu-se um *script* Python adicional para consumir o *endpoint* `/release_dates` da API do TMDB e realizar um comando `MERGE`, mitigando o problema na raiz e demonstrando maturidade prática em Governança de Dados.

### 6. Análise de Dados (Etapa 4.5)

Através do Databricks Lakeview Dashboards, o objetivo do MVP foi concluído respondendo às perguntas mapeadas:

*   **Evolução e Sazonalidade (Painéis: Ano Atual, Faturamento x Ano, Bilheteria por Mês):** O faturamento histórico expôs a retomada pós-crise no recorte 2021-2025, enquanto a quebra mensal revelou picos evidentes de consumo em Janeiro e Julho, fortemente atrelados às férias escolares.
*   **Comportamento do Consumidor e Geografia (Painéis: Dias da Semana, Top 10 Estados, Top 10 Gêneros):** Identificou-se os dias de maior tração nas bilheterias e os gêneros cinematográficos mais rentáveis. A análise geográfica revelou uma concentração maciça do público nos estados do Sudeste, liderada amplamente por São Paulo (SP) e Rio de Janeiro (RJ), mapeando onde a demanda de exibição está consolidada.
*   **Performance de Produto (Painel: Top 10 Públicos):** O ranking confirmou a concentração de mercado em grandes blockbusters e franquias (como Divertida Mente 2 e Homem-Aranha), evidenciando as obras que efetivamente tracionaram o volume absoluto de ingressos no período analisado.

---

1. **Top 10 filmes com maiores públicos:** Quais são os 10 filmes que atraíram as maiores audiências no Brasil?
<img width="1261" height="998" alt="Top 10 Filmes com Maiores Públicos (2)" src="https://github.com/user-attachments/assets/276cc281-0d50-40e9-bddd-a0f4b79b158e" />

    <details>
    <summary><b>🐍 Ver o código: "Quais são os 10 filmes que atraíram as maiores audiências no Brasil?"</b></summary>
    
    ```
    USE CATALOG `dbacademy`;
    USE SCHEMA `default`;
    
    SELECT
        f.titulo_brasil,
        SUM(b.publico) AS total_publico,
        SUM(b.publico) * 20 AS faturamento_estimado_r
    FROM dbacademy.default.gold_features_bilheteria b
    JOIN dbacademy.default.dim_filme f ON b.tmdb_id = f.tmdb_id
    WHERE f.titulo_brasil IS NOT NULL
    AND f.titulo_brasil NOT IN ("HOMEM-ARANHA, SEM VOLTA PARA CASA - A VERSÃO ESTENDIDA", "THE CHOSEN - OS ESCOLHIDOS - TEMPORADA 4 - PARTE 3", "THE CHOSEN - OS ESCOLHIDOS - TEMPORADA 4 - PARTE 4"
    ,"THE CHOSEN - OS ESCOLHIDOS - TEMPORADA 4 - PARTE 2", "THE CHOSEN - OS ESCOLHIDOS - TEMPORADA 4 - PARTE 1", "THE CHOSEN: ÚLTIMA CEIA")
    GROUP BY f.titulo_brasil
    ORDER BY total_publico DESC
    LIMIT 10;
    ```
    </details>
    <br>
    <br>

2. **Ano Atual:** Qual é o desempenho financeiro e de público mês a mês no ano corrente?
<img width="1261" height="566" alt="Público Mensal — 2026" src="https://github.com/user-attachments/assets/23ce9a2a-d387-4b1a-a4e3-56a12f7002e4" />


<details>
<summary><b>🐍 Ver o código: "Qual é o desempenho financeiro e de público mês a mês no ano corrente?"</b></summary>

```
sql
SELECT
  mes_exibicao,
  CASE mes_exibicao
    WHEN 1 THEN 'Jan'
    WHEN 2 THEN 'Fev'
    WHEN 3 THEN 'Mar'
    WHEN 4 THEN 'Abr'
    WHEN 5 THEN 'Mai'
    WHEN 6 THEN 'Jun'
    WHEN 7 THEN 'Jul'
    WHEN 8 THEN 'Ago'
    WHEN 9 THEN 'Set'
    WHEN 10 THEN 'Out'
    WHEN 11 THEN 'Nov'
    WHEN 12 THEN 'Dez'
  END AS mes_nome,
  SUM(publico) AS total_publico,
  SUM(publico) * 20 AS `faturamento_estimado_r$`
FROM
  dbacademy.default.gold_features_bilheteria
WHERE
  ano_exibicao = 2026
GROUP BY
  mes_exibicao,
  mes_nome
ORDER BY
  mes_exibicao ASC
```
</details>
<br>
<br>

3. **Faturamento x Ano:** Qual é a evolução histórica do faturamento anual do setor?
<img width="1261" height="708" alt="Faturamento Estimado por Ano (2021-2025)" src="https://github.com/user-attachments/assets/10292610-51fc-4f3c-8eb8-3d1c0006d2b1" />

<details>
<summary><b>🐍 Ver o código: "Qual é a evolução histórica do faturamento anual do setor?"</b></summary>

```
sql
USE CATALOG `dbacademy`;
USE SCHEMA `default`;

SELECT
    ano_exibicao,
    SUM(publico) AS total_publico,
    SUM(publico) * 20 AS `faturamento_estimado_r$`
FROM dbacademy.default.gold_features_bilheteria
WHERE ano_exibicao BETWEEN 2021 AND 2025
GROUP BY ano_exibicao
ORDER BY ano_exibicao ASC;
```
</details>
<br>
<br>

4. **Bilheteria entre os meses (2021 a 2026):** Como a sazonalidade afeta o desempenho das salas ao longo dos anos?
<img width="1261" height="528" alt="Bilheteria entre os meses dos anos entre 2021 a 2026" src="https://github.com/user-attachments/assets/91de2d53-b601-4443-8225-5822543fa804" />

<details>
<summary><b>🐍 Ver o código: "Como a sazonalidade afeta o desempenho das salas ao longo dos anos?"</b></summary>

```
sql
USE CATALOG `dbacademy`;
USE SCHEMA `default`;

SELECT
    mes_exibicao,
    CASE mes_exibicao
        WHEN 1 THEN 'Jan' WHEN 2 THEN 'Fev' WHEN 3 THEN 'Mar'
        WHEN 4 THEN 'Abr' WHEN 5 THEN 'Mai' WHEN 6 THEN 'Jun'
        WHEN 7 THEN 'Jul' WHEN 8 THEN 'Ago' WHEN 9 THEN 'Set'
        WHEN 10 THEN 'Out' WHEN 11 THEN 'Nov' WHEN 12 THEN 'Dez'
    END AS mes_nome,
    SUM(publico) AS total_publico,
    SUM(publico) * 20 AS `estimativa_renda_r$`
FROM dbacademy.default.gold_features_bilheteria
GROUP BY mes_exibicao, mes_nome
ORDER BY mes_exibicao;
```
</details>
<br>
<br>

5. **Dias da Semana que mais vendem:** Quais dias da semana concentram o maior volume de vendas de ingressos?
<img width="1261" height="494" alt="Público por Dia da Semana (1)" src="https://github.com/user-attachments/assets/73405d08-c356-4e0d-a835-9debbb83205e" />

<details>
<summary><b>🐍 Ver o código: "Como a sazonalidade afeta o desempenho das salas ao longo dos anos?"</b></summary>

```
sql
USE CATALOG `dbacademy`;
USE SCHEMA `default`;

SELECT
    DAYOFWEEK(data_exibicao) AS dia_semana_num,
    CASE DAYOFWEEK(data_exibicao)
        WHEN 1 THEN 'Domingo' WHEN 2 THEN 'Segunda' WHEN 3 THEN 'Terça'
        WHEN 4 THEN 'Quarta'  WHEN 5 THEN 'Quinta'  WHEN 6 THEN 'Sexta' WHEN 7 THEN 'Sábado'
    END AS dia_da_semana,
    SUM(publico) AS total_publico,
    SUM(publico) * 20 AS `estimativa_renda_r$`
FROM dbacademy.default.gold_features_bilheteria
GROUP BY dia_semana_num, dia_da_semana
ORDER BY dia_semana_num;
```
</details>
<br> 
<br>

6. **Distribuição Geográfica:** Como o público consumidor de cinema está distribuído entre os estados brasileiros (UF)?
<img width="1261" height="708" alt="Top 10 Estados - Distribuição de Público" src="https://github.com/user-attachments/assets/ce40bc67-474f-4073-b0fa-8dd58fd4bc63" />

<details>
<summary><b>🐍 Ver o código: "Como o público consumidor de cinema está distribuído entre os estados brasileiros (UF)?"</b></summary>

```
sql
USE CATALOG `dbacademy`;

USE SCHEMA `default`;

SELECT
  uf,
  SUM(publico) AS total_publico,
  SUM(publico) * 20 AS `estimativa_renda_r$`
FROM
  dbacademy.default.gold_features_bilheteria
GROUP BY
  uf
ORDER BY
  total_publico DESC
LIMIT 10;
```
</details>
<br>
<br>

7. **Top 10 Gêneros:** Quais são os 10 gêneros cinematográficos mais rentáveis e populares?
<img width="1261" height="708" alt="Estimativa de Renda por Gênero (1)" src="https://github.com/user-attachments/assets/f0f2207e-0a74-4d26-ae0e-6ea486c47fa8" />

<details>
<summary><b>🐍 Ver o código: "Quais são os 10 gêneros cinematográficos mais rentáveis e populares?"</b></summary>

```
sql
USE CATALOG `dbacademy`;
USE SCHEMA `default`;

SELECT
    g.nome_genero,
    SUM(f.publico) AS total_publico,
    SUM(f.publico) * 20 AS `estimativa_renda_r$`
FROM dbacademy.default.gold_features_bilheteria f
JOIN dbacademy.default.filme_genero fg ON f.tmdb_id = fg.tmdb_id
JOIN dbacademy.default.dim_genero g ON fg.id_genero = g.id_genero
GROUP BY g.nome_genero
ORDER BY `estimativa_renda_r$` DESC
LIMIT 10;
```
</details>
<br>
<br>

### 7. Autoavaliação

O projeto cumpriu integralmente seu objetivo de entregar um pipeline de dados funcional estruturado na nuvem, simulando um ambiente corporativo real. A Arquitetura Medalhão mostrou-se essencial para refinar dados brutos até a entrega de métricas confiáveis para tomada de decisão. 

A maior dificuldade foi lidar com o trabalho pesado de *Data Wrangling* (concatenação de anos, correção de encoding UTF-8 e integração via API) e as limitações das fontes primárias, como a ausência de metadados regionais na rota padrão do TMDB. Isso exigiu um esforço extra de engenharia para cobrir o buraco de dados sem afetar a análise financeira. Como trabalho futuro, planeja-se automatizar a coleta 100% via *Web Scraping* ou integrar *endpoints* governamentais para cruzamento nativo e automatizado das classificações indicativas.

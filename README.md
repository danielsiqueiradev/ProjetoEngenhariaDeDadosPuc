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

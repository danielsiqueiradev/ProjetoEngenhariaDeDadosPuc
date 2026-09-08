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

### 5. Qualidade de Dados (Etapa 4.5)

Durante a exploração dos dados, detectou-se uma falha de completude crítica: 146 registros cruciais (representando mais de 600 milhões de ingressos) apresentavam valor `NULL` na coluna de classificação etária na camada Silver, pois essa informação específica não vinha na rota principal da API do TMDB.

**Resolução:** Em vez de excluir os dados (o que corromperia o faturamento total da Ancine) ou inferir valores irreais, utilizou-se a função SQL `COALESCE` para categorizá-los como "Não Informada". Paralelamente, desenvolveu-se um *script* Python adicional para consumir o *endpoint* `/release_dates` da API do TMDB e realizar um comando `MERGE`, mitigando o problema na raiz e demonstrando maturidade prática em Governança de Dados.

### 6. Análise de Dados (Etapa 4.5)

Através do Databricks Lakeview Dashboards, o objetivo do MVP foi concluído respondendo às perguntas mapeadas:

*   **Evolução e Sazonalidade (Painéis: Ano Atual, Faturamento x Ano, Bilheteria por Mês):** O faturamento histórico expôs a retomada pós-crise no recorte 2021-2025, enquanto a quebra mensal revelou picos evidentes de consumo em Janeiro e Julho, fortemente atrelados às férias escolares.
*   **Comportamento do Consumidor e Geografia (Painéis: Dias da Semana, Top 10 Estados, Top 10 Gêneros):** Identificou-se os dias de maior tração nas bilheterias e os gêneros cinematográficos mais rentáveis. A análise geográfica revelou uma concentração maciça do público nos estados do Sudeste, liderada amplamente por São Paulo (SP) e Rio de Janeiro (RJ), mapeando onde a demanda de exibição está consolidada.
*   **Performance de Produto (Painel: Top 10 Públicos):** O ranking confirmou a concentração de mercado em grandes blockbusters e franquias (como Divertida Mente 2 e Homem-Aranha), evidenciando as obras que efetivamente tracionaram o volume absoluto de ingressos no período analisado.

1. **Top 10 filmes com maiores públicos:** Quais são os 10 filmes que atraíram as maiores audiências no Brasil?
<img width="50%" alt="Top 10 Filmes com Maiores Públicos" src="https://github.com/user-attachments/assets/247b0b02-a7b4-4f20-b7e8-7eceb21fbf61" />

2. **Ano Atual:** Qual é o desempenho financeiro e de público mês a mês no ano corrente?
<img width="50%" height="618" alt="Público e Faturamento por Mês - 2026" src="https://github.com/user-attachments/assets/1fb91d39-7f8c-4f8a-8d65-55cf39220f95" />

3. **Faturamento x Ano:** Qual é a evolução histórica do faturamento anual do setor?
<img width="50%" height="708" alt="Faturamento Estimado por Ano (2021-2025)" src="https://github.com/user-attachments/assets/10292610-51fc-4f3c-8eb8-3d1c0006d2b1" />

4. **Bilheteria entre os meses (2021 a 2026):** Como a sazonalidade afeta o desempenho das salas ao longo dos anos?
<img width="50%" height="528" alt="Bilheteria entre os meses dos anos entre 2021 a 2026" src="https://github.com/user-attachments/assets/91de2d53-b601-4443-8225-5822543fa804" />

5. **Dias da Semana que mais vendem:** Quais dias da semana concentram o maior volume de vendas de ingressos?
<img width="50%"  height="618" alt="Público por Dia da Semana" src="https://github.com/user-attachments/assets/7e2de5c4-91ba-48b8-9b5a-9e183796ec53" />
  
6. **Distribuição Geográfica:** Como o público consumidor de cinema está distribuído entre os estados brasileiros (UF)?
<img width="50%" height="708" alt="Top 10 Estados - Distribuição de Público" src="https://github.com/user-attachments/assets/ce40bc67-474f-4073-b0fa-8dd58fd4bc63" />

7. **Top 10 Gêneros:** Quais são os 10 gêneros cinematográficos mais rentáveis e populares?
<img width="50%" height="708" alt="Estimativa de Renda por Gênero (1)" src="https://github.com/user-attachments/assets/f0f2207e-0a74-4d26-ae0e-6ea486c47fa8" />

### 7. Autoavaliação

O projeto cumpriu integralmente seu objetivo de entregar um pipeline de dados funcional estruturado na nuvem, simulando um ambiente corporativo real. A Arquitetura Medalhão mostrou-se essencial para refinar dados brutos até a entrega de métricas confiáveis para tomada de decisão. 

A maior dificuldade foi lidar com o trabalho pesado de *Data Wrangling* (concatenação de anos, correção de enconding UTF-8 e integração via API) e as limitações das fontes primárias, como a ausência de metadados regionais na rota padrão do TMDB. Isso exigiu um esforço extra de engenharia para cobrir o buraco de dados sem afetar a análise financeira. Como trabalho futuro, planeja-se automatizar a coleta 100% via *Web Scraping* ou integrar *endpoints* governamentais para cruzamento nativo e automatizado das classificações indicativas.

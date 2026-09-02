# MVP: Pipeline de Dados de Bilheteria de Cinema no Databricks

### 1. Contexto de Negócios e Perguntas (Etapa 2 e 4.1)

**Contexto de Negócio:**
O mercado de exibição cinematográfica precisa entender onde estão seus gargalos operacionais e onde a receita é efetivamente gerada. Este projeto analisa dados de bilheteria e exibições no Brasil, cruzando o volume de público com metadados dos filmes. O objetivo é fornecer uma base para tomadas de decisão sobre alocação de salas e impacto da classificação indicativa no faturamento. 

**Estrutura dos Dados Brutos e Licença:**
Os dados brutos consistem em fatos de bilheteria diária e metadados de filmes (títulos, IDs, notas). As informações são oriundas de Dados Abertos (como a Ancine, sob a licença de Dados Abertos do Governo Federal) e complementadas via API do TMDB (The Movie Database), que permite uso não comercial.

**Perguntas de Negócio:**
1. Qual a evolução histórica do faturamento anual (2021-2025) e o cenário mês a mês do ano atual (2026)?
2. Como a sazonalidade impacta a receita mensal das redes de cinema?
3. Qual é o impacto financeiro da classificação etária (censura) na arrecadação?
4. Quais são os 10 filmes de maior sucesso de público no mercado brasileiro?

### 2. Carga dos Dados (Etapa 4.2)

Para simular um ambiente corporativo real, os dados foram extraídos de suas fontes originais (arquivos CSV de órgãos oficiais e requisições em formato JSON via API do TMDB) e carregados diretamente no DBFS/Volumes do Databricks. Essa etapa representa a camada **Bronze**, onde o dado é mantido no seu formato bruto original, preservando a rastreabilidade caso seja necessário auditar a origem.

*(Script de ingestão referenciado no repositório: `ingestao_bronze.py` ou `notebook_bronze.sql`)*

### 3. Modelagem e Catálogo de Dados (Etapa 4.3)

A modelagem adotada segue o princípio do *Lakehouse*, estruturando os dados de forma otimizada para consultas analíticas. Utilizamos uma abordagem de *Star Schema* adaptada para o Delta Lake:

*   **dim_filme (Dimensão - Silver):** Tabela contendo os atributos dos filmes.
*   **gold_features_bilheteria (Fato - Gold):** Tabela central agregando métricas de negócio.

**Catálogo de Dados:**
*   **Tabela `dim_filme`**:
    *   `tmdb_id` (Integer): Chave primária do filme oriunda do TMDB.
    *   `titulo_brasil` (String): Título oficial do filme no mercado brasileiro.
    *   `classificacao_etaria` (String): Censura oficial no Brasil (Domínio: Livre, 10, 12, 14, 16, 18).
*   **Tabela `gold_features_bilheteria`**:
    *   `tmdb_id` (Integer): Chave estrangeira ligando à dimensão do filme.
    *   `data_exibicao` (Date): Data em que a sessão ocorreu.
    *   `publico` (Integer): Quantidade de ingressos vendidos (Domínio: > 0).
    *   `faturamento_estimado_r$` (Decimal): Métrica de negócio gerada na modelagem multiplicando o público pelo ticket médio estimado.

> [INSERIR PRINT AQUI: Screenshot do Data Explorer / Unity Catalog do Databricks mostrando os schemas das tabelas]

### 4. Pipeline de Dados (Etapa 4.4)

O processo de ETL (Extração, Transformação e Carga) foi centralizado em *Notebooks* no Databricks, ramificando o processamento em etapas claras, implementando a Arquitetura Medalhão nativa:

1.  **Extract:** Leitura dos dados armazenados na camada Bronze.
2.  **Transform:** Limpeza, remoção de duplicatas, e normalização de nomes na camada **Silver** (`dim_filme`). Na camada **Gold**, as tabelas foram unidas (`JOIN`) e enriquecidas com agregações de negócio.
3.  **Load:** As tabelas finais foram salvas fisicamente em formato Delta, garantindo transações ACID e performance.

*(Scripts referenciados no repositório: `etl_silver.py`, `etl_gold.sql`)*

> [INSERIR PRINT AQUI: Screenshot do Databricks provando que as tabelas "dim_filme" e "gold_features_bilheteria" estão persistidas fisicamente]

### 5. Qualidade de Dados (Etapa 4.5)

Durante a análise de qualidade, que precede a resposta às perguntas, detectou-se uma falha de completude: 146 registros cruciais (representando mais de 600 milhões de ingressos) apresentavam valor `NULL` na coluna `classificacao_etaria` na camada Silver.

**Resolução:**
1.  **Tratamento no Dashboard:** Em vez de excluir os dados (o que distorceria o faturamento total) ou inferir valores irreais, utilizou-se a transformação SQL `COALESCE(classificacao_etaria, 'Não Informada')`. Isso garante que o problema de Governança de Dados seja visível para a área de negócios.
2.  **Ação de Engenharia:** Foi desenvolvido um *script* Python complementar que consome o *endpoint* `/release_dates` da API do TMDB, fazendo um *loop* nos IDs nulos e realizando um comando `MERGE` (Update) direto na tabela Delta para enriquecer o banco.

### 6. Análise de Dados (Etapa 4.5)

Através do Databricks Lakeview Dashboards, com consultas em SQL, o objetivo do projeto foi concluído:

*   **1. Evolução Histórica e Ano Atual:** A análise segmentada mostra a retomada do faturamento entre 2021 e 2025 (Gráfico de Barras), e o monitoramento em tempo real (YTD) de 2026 (Gráfico de Área), evidenciando a saúde financeira corrente da operação.
*   **2. Sazonalidade:** O agrupamento mensal comprova picos brutais de bilheteria em Janeiro e Julho, alinhados às férias escolares brasileiras.
*   **3. Impacto da Censura:** Filmes com classificação "12 anos" e "Livre" englobam a maior fatia rastreável de faturamento, mostrando que a operação do cinema é amplamente dependente do público familiar. (O alerta da qualidade de dados está presente na fatia "Não Informada").
*   **4. Top 10 Blockbusters:** O cruzamento com a tabela de dimensão permitiu ranquear as 10 obras que mais arrastaram público para as salas, mostrando concentração de mercado em grandes franquias. *(Nota Metodológica: Devido à ausência da coluna original de renda na fonte, adotou-se a premissa de negócio de ticket médio de R$ 20,00 por ingresso).*

> [INSERIR PRINT AQUI: Screenshots do Lakeview Dashboards com os 4 gráficos finais]

### 7. Autoavaliação

O projeto cumpriu integralmente seu objetivo principal de entregar um pipeline de dados funcional e um dashboard executivo estruturado na nuvem, simulando um ambiente corporativo real. A Arquitetura Medalhão se provou eficiente na transição do dado sujo até a métrica de negócio.

A maior dificuldade encontrada foi a limitação da fonte primária (TMDB), que não enviava a censura regional brasileira na rota padrão da API, exigindo desenvolvimento em Python para tratar o gargalo e evitar buracos massivos no BI. 

Como trabalho futuro, planeja-se integrar a API de Dados Abertos da Ancine para cruzamento de chaves e substituição do uso do `COALESCE`, cobrindo 100% da base histórica com certificações etárias oficiais brasileiras.

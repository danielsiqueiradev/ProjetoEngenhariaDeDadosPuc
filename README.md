# MVP: Pipeline de Dados de Bilheteria de Cinema

**Plataforma Utilizada:** Databricks (Lakehouse Architecture) / Lakeview Dashboards.

**Objetivo:** 
O presente MVP (Produto Mínimo Viável) tem como objetivo construir um pipeline de dados analítico de ponta a ponta na nuvem. O foco é processar dados brutos do mercado cinematográfico brasileiro, cruzá-los com metadados de filmes, e estruturá-los para responder a perguntas estratégicas de negócio sobre faturamento, sazonalidade e comportamento do público, culminando em um painel de inteligência de mercado executivo.

<img width="1024" height="454" alt="Sem título" src="https://github.com/user-attachments/assets/ab253683-3a1b-4ea5-8868-49d4c32207c5" />

---

### 1. Contexto de Negócios e Perguntas (Etapas 2 e 4.1)

**Contexto de Negócio:**
O mercado de exibição cinematográfica precisa entender onde estão seus gargalos operacionais e onde a receita é efetivamente gerada. Este projeto analisa dados de bilheteria e exibições no Brasil, cruzando o volume de público com metadados dos filmes. 

**Estrutura dos Dados Brutos e Licença:**
A fonte primária e majoritária dos dados é a Agência Nacional do Cinema (Ancine), sob a licença de Dados Abertos do Governo Federal. Para o enriquecimento do catálogo, utilizou-se a API do TMDB (The Movie Database), que permite uso não comercial.

**Perguntas de Negócio (Mapeadas para os Dashboards):**
1. **Top 10 filmes com maiores públicos:** Quais são os 10 filmes que atraíram as maiores audiências no Brasil?
2. **Ano Atual:** Qual é o desempenho financeiro e de público mês a mês no ano corrente?
3. **Faturamento x Ano:** Qual é a evolução histórica do faturamento anual do setor?
4. **Bilheteria entre os meses (2021 a 2026):** Como a sazonalidade afeta o desempenho das salas ao longo dos anos?
5. **Relação entre Nota IMDB x Público:** A percepção de qualidade (Nota Média) tem correlação direta com o sucesso de bilheteria?
6. **Dias da Semana que mais vendem:** Quais dias da semana concentram o maior volume de vendas de ingressos?
7. **Filmes que mais renderam:** Quais obras geraram a maior arrecadação financeira absoluta?
8. **População que mais consome cinema no Brasil:** Qual é o perfil de consumo e impacto financeiro baseado na classificação etária (censura)?
9. **Top 10 Gêneros:** Quais são os 10 gêneros cinematográficos mais rentáveis e populares?

### 2. Coleta e Carga dos Dados (Etapa 4.2)

A ingestão de dados envolveu um trabalho prévio de *Data Wrangling* para garantir a viabilidade analítica antes da subida para o Databricks:
1. **Extração Primária:** Download da base histórica completa de bilheteria pública da Ancine (de 2014 até o presente).
2. **Filtro Temporal e Limpeza:** Para manter a relevância do mercado recente, os dados anteriores a 2021 foram excluídos, estabelecendo um recorte de 5 anos de bilheteria. Colunas sem valor analítico foram removidas para otimizar o processamento.
3. **Consolidação:** Os arquivos foram concatenados em um único arquivo CSV e passaram por correção de *encoding* para UTF-8, evitando erros em caracteres especiais dos títulos.
4. **Enriquecimento via API:** Foi construído um *script* de conexão com a API do TMDB para buscar metadados específicos de cada obra, incorporando ao *dataset* os seguintes campos: `TITULO_BRASIL`, `TMDB_ID`, `POPULARIDADE`, `NOTA_MEDIA`, `VOTOS`, `DATA_LANCAMENTO`, `ORCAMENTO_USD` e `GENEROS`.

Após esse tratamento inicial, o CSV consolidado e enriquecido foi carregado diretamente no DBFS/Volumes do Databricks, representando a camada **Bronze** (landing zone).

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
*   **Comportamento do Consumidor (Painéis: Dias da Semana, População que mais consome, Top 10 Gêneros):** Identificou-se os dias de maior tração nas bilheterias. A análise demográfica cruzada com a censura provou que o mercado é sustentado pelo público familiar (classificações Livre e 12 anos).
*   **Performance de Produto (Painéis: Top 10 Públicos, Filmes que mais renderam, Relação Nota x Público):** O ranking confirmou a concentração de mercado em grandes blockbusters. O cruzamento das métricas da API (Nota Média e Votos) com o público real da Ancine permitiu avaliar se o sucesso de crítica reflete diretamente no sucesso de caixa nacional.

> [INSERIR PRINTS AQUI: Screenshots de 2 ou 3 gráficos principais gerados no Databricks]

### 7. Autoavaliação

O projeto cumpriu integralmente seu objetivo de entregar um pipeline de dados funcional estruturado na nuvem, simulando um ambiente corporativo real. A Arquitetura Medalhão mostrou-se essencial para refinar dados brutos até a entrega de métricas confiáveis para tomada de decisão. 

A maior dificuldade foi lidar com o trabalho pesado de *Data Wrangling* (concatenação de anos, correção de enconding UTF-8 e integração via API) e as limitações das fontes primárias, como a ausência de metadados regionais na rota padrão do TMDB. Isso exigiu um esforço extra de engenharia para cobrir o buraco de dados sem afetar a análise financeira. Como trabalho futuro, planeja-se automatizar a coleta 100% via *Web Scraping* ou integrar *endpoints* governamentais para cruzamento nativo e automatizado das classificações indicativas.

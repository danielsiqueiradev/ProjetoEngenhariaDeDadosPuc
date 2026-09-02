# MVP: Pipeline de Dados de Bilheteria de Cinema

**Plataforma Utilizada:** Databricks (Lakehouse Architecture) / Lakeview Dashboards.

**Objetivo:** 
O presente MVP (Produto Mínimo Viável) tem como objetivo construir um pipeline de dados analítico de ponta a ponta na nuvem. O foco é processar dados brutos do mercado cinematográfico brasileiro, cruzá-los com metadados de filmes, e estruturá-los para responder a perguntas estratégicas de negócio sobre faturamento, sazonalidade e comportamento do público, culminando em um painel de inteligência de mercado executivo.

---

### 1. Contexto de Negócios e Perguntas (Etapas 2 e 4.1)

**Contexto de Negócio:**
O mercado de exibição cinematográfica precisa entender onde estão seus gargalos operacionais e onde a receita é efetivamente gerada. Este projeto analisa dados de bilheteria e exibições no Brasil, cruzando o volume de público com metadados dos filmes. 

**Estrutura dos Dados Brutos e Licença:**
Os dados brutos consistem em fatos de bilheteria diária e metadados de filmes (títulos, IDs, notas). As informações são oriundas de Dados Abertos (Ancine, sob a licença de Dados Abertos do Governo Federal) e complementadas via API do TMDB (The Movie Database), que permite uso não comercial.

**Perguntas de Negócio (Mapeadas para os Dashboards):**
1. **Top 10 filmes com maiores públicos:** Quais são os 10 filmes que atraíram as maiores audiências no Brasil?
2. **Ano Atual:** Qual é o desempenho financeiro e de público mês a mês no ano corrente?
3. **Faturamento x Ano:** Qual é a evolução histórica do faturamento anual do setor?
4. **Bilheteria entre os meses (2021 a 2026):** Como a sazonalidade afeta o desempenho das salas ao longo dos anos?
5. **Relação entre Nota IMDB x Público:** A percepção de qualidade (Nota IMDB) tem correlação direta com o sucesso de bilheteria?
6. **Dias da Semana que mais vendem:** Quais dias da semana concentram o maior volume de vendas de ingressos?
7. **Filmes que mais renderam:** Quais obras geraram a maior arrecadação financeira absoluta?
8. **População que mais consome cinema no Brasil:** Qual é o perfil de consumo e impacto financeiro baseado na classificação etária (censura)?
9. **Top 10 Gêneros:** Quais são os 10 gêneros cinematográficos mais rentáveis e populares?

### 2. Carga dos Dados (Etapa 4.2)

Para simular um ambiente corporativo, os dados foram extraídos de suas fontes originais (arquivos CSV e requisições JSON via API do TMDB) e carregados diretamente no DBFS/Volumes do Databricks. Essa etapa representa a camada **Bronze**, onde o dado é mantido no seu formato bruto original, preservando a rastreabilidade caso seja necessário auditar a origem.
*(Script de ingestão referenciado no repositório).*

### 3. Modelagem e Catálogo de Dados (Etapa 4.3)

A modelagem adotada segue o princípio do *Lakehouse*, utilizando uma abordagem de *Star Schema* adaptada para o Delta Lake:
*   **dim_filme (Dimensão - Silver):** Tabela contendo atributos descritivos dos filmes (Gênero, Nota IMDB, Título, Censura).
*   **gold_features_bilheteria (Fato - Gold):** Tabela central agregando métricas de negócio e chaves de tempo.

**Catálogo de Dados (Resumo das principais métricas):**
*   **`dim_filme`**: `tmdb_id` (Integer - PK), `titulo_brasil` (String), `classificacao_etaria` (String), `nota_imdb` (Decimal), `genero` (String).
*   **`gold_features_bilheteria`**: `tmdb_id` (Integer - FK), `data_exibicao` (Date), `dia_semana` (String), `publico` (Integer), `faturamento_estimado_r$` (Decimal - Métrica de negócio gerada multiplicando o público por um ticket médio de R$ 20,00).

> [INSERIR PRINT AQUI: Screenshot do Data Explorer / Unity Catalog do Databricks mostrando os schemas]

### 4. Pipeline de Dados (Etapa 4.4)

O processo de ETL (Extração, Transformação e Carga) foi centralizado em *Notebooks* no Databricks, implementando a Arquitetura Medalhão:
1.  **Extract:** Leitura dos dados armazenados na camada Bronze.
2.  **Transform:** Limpeza, remoção de duplicatas, tratamento de datas (para extrair dias da semana) e normalização na camada **Silver**. Na camada **Gold**, as tabelas foram unidas (`JOIN`) e enriquecidas com agregações de negócio (cálculo de faturamento).
3.  **Load:** As tabelas finais foram salvas fisicamente em formato Delta (`.format("delta").saveAsTable(...)`), garantindo performance para os painéis analíticos.

> [INSERIR PRINT AQUI: Screenshot do Databricks provando que as tabelas estão persistidas]

### 5. Qualidade de Dados (Etapa 4.5)

Durante a exploração dos dados, detectou-se uma falha de completude crítica: 146 registros cruciais (representando mais de 600 milhões de ingressos) apresentavam valor `NULL` na coluna de classificação etária na camada Silver.

**Resolução:** Em vez de excluir os dados (o que corromperia o faturamento) ou inferir valores irreais, utilizou-se a função SQL `COALESCE` para categorizá-los como "Não Informada". Paralelamente, desenvolveu-se um *script* Python para consumir o *endpoint* `/release_dates` da API do TMDB e realizar um comando `MERGE`, mitigando o problema na raiz e mostrando maturidade em Governança de Dados.

### 6. Análise de Dados (Etapa 4.5)

Através do Databricks Lakeview Dashboards, o objetivo do MVP foi concluído respondendo às perguntas mapeadas:

*   **Evolução e Sazonalidade (Painéis: Ano Atual, Faturamento x Ano, Bilheteria por Mês):** O faturamento histórico expôs a retomada pós-crise, enquanto a quebra mensal revelou picos evidentes de consumo em Janeiro e Julho, fortemente atrelados às férias escolares.
*   **Comportamento do Consumidor (Painéis: Dias da Semana, População que mais consome, Top 10 Gêneros):** Identificou-se os dias de maior tração nas bilheterias. A análise demográfica cruzada com a censura provou que o mercado é sustentado pelo público familiar (classificações Livre e 12 anos).
*   **Performance de Produto (Painéis: Top 10 Públicos, Filmes que mais renderam, Nota IMDB x Público):** O ranking confirmou a concentração de mercado em grandes blockbusters e franquias, permitindo avaliar se a aprovação crítica reflete diretamente no sucesso financeiro.

> [INSERIR PRINTS AQUI: Screenshots de 2 ou 3 gráficos principais gerados no Databricks]

### 7. Autoavaliação

O projeto cumpriu integralmente seu objetivo de entregar um pipeline de dados funcional estruturado na nuvem, simulando um ambiente corporativo real. A Arquitetura Medalhão mostrou-se essencial para refinar dados brutos até a entrega de métricas confiáveis para tomada de decisão. 

A maior dificuldade foi lidar com a inconsistência da fonte primária, que omitia metadados regionais importantes (censura), exigindo esforço extra de engenharia via API para cobrir o buraco de dados sem afetar a análise financeira. Como trabalho futuro, planeja-se integrar a API de Dados Abertos da Ancine para cruzamento robusto de chaves, enriquecendo as dimensões de forma nativa e automatizada.

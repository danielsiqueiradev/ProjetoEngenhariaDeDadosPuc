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

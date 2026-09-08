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

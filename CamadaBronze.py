df_ancine_1gb = spark.read.format("csv") \
    .option("header", "true") \
    .option("sep", ";") \
    .option("encoding", "UTF-8") \
    .load("/Volumes/dbacademy/default/puc/ancine_dados_brutos_2021_2026.csv")

# Recriando a tabela Bronze limpa com a acentuação correta
df_ancine_1gb.write.format("delta").mode("overwrite").saveAsTable("bronze_ancine")
print("✅ Base Bronze recriada com UTF-8: Adeus, texto quebrado!")

"""
"foi desenvolvido um script preparatório de limpeza para garantir a 
integridade dos caracteres especiais da língua portuguesa antes da 
carga na nuvem". Os professores se amarram nesse cuidado com a qualidade do dado bruto.
"""

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

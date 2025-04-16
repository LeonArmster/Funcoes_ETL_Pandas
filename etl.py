# Bibliotecas
import pandas as pd
import os
import glob

# função para extract que le e consolida os json
def extrair_dados(path:str) -> pd.DataFrame:
    arquivos_json = glob.glob(os.path.join(path, '*.json'))
    df_list = [pd.read_json(arquivo) for arquivo in arquivos_json]
    df_total = pd.concat(df_list, ignore_index=True)
    return df_total


# Função de transformação
def calcular_kpi_total_vendas(df:pd.DataFrame) -> pd.DataFrame:
    df['Total'] = df['Quantidade'] * df['Venda']
    return df



# Função de saída
def salvar_dados(df:pd.DataFrame, format_saida:list):
    """
    Objetivo: definir o formato de salvamento podendo ser csv, parquet ou os 2
    df: dataframe trabalhado
    format_saida: formato que os dados serão salvos
    """
    for formato in format_saida:
        if formato == 'csv':
            df.to_csv('dados.csv', index=False)
        if formato == 'parquet':
            df.to_parquet('dados.parquet', index=False)


# Pipeline Usuario Final
def pipeline_calcular_kpi_vendas_consolidado(path:str, formato:list):
    df = extrair_dados(path=path)
    df_calculado = calcular_kpi_total_vendas(df=df)
    salvar_dados(df=df_calculado, format_saida=formato)



if __name__ == "__main__":
    pasta = 'data'
    df = extrair_dados(path=pasta)
    df_calculado = calcular_kpi_total_vendas(df=df)
    formato_saida = ['csv', 'parquet']
    salvar_dados(df=df_calculado, format_saida=formato_saida)
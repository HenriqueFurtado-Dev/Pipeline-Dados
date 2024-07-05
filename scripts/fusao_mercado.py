import json
import csv

def leitura_json(path_json):

    dados_json = []
    with open(path_json, 'r') as file:
        dados_json = json.load(file)
    return dados_json

def leitura_csv(path_csv):

    dados_csv = []
    with open(path_csv, 'r') as file:
        spamreader = csv.DictReader(file, delimiter=',')
        for row in spamreader:
            dados_csv.append(row)

    return dados_csv

def leitura_dados(path, tipo_arquivo):
    dados = []

    if tipo_arquivo == 'json':
        dados = leitura_json(path)

    elif tipo_arquivo == 'csv':
        dados = leitura_csv(path)

    return dados

def get_columns(dados):
    return list(dados[-1].keys())

def rename_columns(dados, key_mapping):
    new_dados_csv = []

    for old_dict in dados:
        dict_temp = {}
        for old_key, value in old_dict.items():
            dict_temp[key_mapping[old_key]] = value
        new_dados_csv.append(dict_temp)

    return new_dados_csv

def size_data(dados):
    return len(dados)

def join(dadosA, dadosB):
    combined_list = []
    combined_list.extend(dadosA)
    combined_list.extend(dadosB)
    return combined_list

def transformando_dados_tabela(dados, nome_colunas):
    dados_combinados_tabela = [nome_colunas]

    for row in dados:
        linha = []
        for coluna in nome_colunas:
            linha.append(row.get(coluna, 'Indisponivel'))
        dados_combinados_tabela.append(linha)
    
    return dados_combinados_tabela

def salvando_dados(dados, path):
    with open(path, 'w') as file:
        writer = csv.writer(file)
        writer.writerows(dados)

# Iniciando leitura
path_json = 'pipeline_dados\data_raw\dados_empresaA.json'
path_csv = 'pipeline_dados\data_raw\dados_empresaB.csv'

dados_json = leitura_dados(path_json, 'json')
print(dados_json[0])
print(f'Tamanho dos dados: {size_data(dados_json)}\n')

dados_csv = leitura_dados(path_csv, 'csv')
print(dados_csv[0])
print(f'Tamanho dos dados: {size_data(dados_csv)}\n')

nome_colunas_json = get_columns(dados_json)
print(nome_colunas_json)

nome_colunas_csv = get_columns(dados_csv)
print(nome_colunas_csv)

key_mapping = {'Nome do Item': 'Nome do Produto',
                'Classificacao do Produto': 'Categoria do Produto',
                'Valor em Reais (R$)': 'Preço do Produto (R$)',
                'Quantidade em Estoque': 'Quantidade em Estoque',
                'Nome da Loja': 'Filial',
                'Data da Venda': 'Data da Venda'}

# Transformando dados
dados_csv = rename_columns(dados_csv, key_mapping)
nome_colunas_csv = get_columns(dados_csv)
print(f'Colunas: {nome_colunas_csv}\n')

dados_fusao = join(dados_csv, dados_json)
print(size_data(dados_fusao))

# Salvando dados
dados_fusao_tabela = transformando_dados_tabela(dados_fusao, nome_colunas_json)

path_dados_combinados = 'pipeline_dados\data_processed\dados-salvos.csv'

salvando_dados(dados_fusao_tabela, path_dados_combinados)
from flask import Flask, request, jsonify
from flask_cors import CORS
import pymongo
import pdfplumber
import tempfile
import os
from datetime import datetime

app = Flask(__name__)
CORS(app)

# Conexão com o MongoDB
client = pymongo.MongoClient("mongodb+srv://caique:300904Ca!@cluster0.wyxgl.mongodb.net/")
db = client["comissao"]
collection = db["comissoes"]

# Função para buscar vendedores no banco de dados
def get_vendedores_by_cliente(nome_cliente):
    try:
        segurado = collection.find_one({'CLIENTE': nome_cliente})
        if segurado:
            return segurado.get('TODOS VENDEDORES', [])
        else:
            return []
    except Exception as e:
        print(f"Erro ao buscar vendedores: {e}")
        return []

# Função para normalizar strings (remoção de espaços extras e capitalização uniforme)
def normalize_name(name):
    return ' '.join(name.strip().lower().split())

@app.route('/upload', methods=['POST'])
def upload_pdf():
    if 'files' not in request.files:
        return jsonify({'error': 'Nenhum arquivo foi enviado'}), 400

    pdf_files = request.files.getlist('files')

    if len(pdf_files) == 0:
        return jsonify({'error': 'Nenhum arquivo foi enviado'}), 400

    segurados = {}
    data_atual = datetime.now().strftime("%Y-%m-%d")  # Armazenar a data de upload

    try:
        for pdf_file in pdf_files:
            temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.pdf')
            try:
                pdf_file.save(temp_file.name)

                with pdfplumber.open(temp_file.name) as pdf:
                    seguradora = None
                    data_pagamento = None  # Inicializar a variável da data de pagamento
                    if len(pdf.pages) > 0:
                        first_page = pdf.pages[0]
                        text = first_page.extract_text()

                        lines = text.split('\n')
                        for line in lines:
                            if "Relatório de Comissões Diárias - Todas as comissões" in line:
                                seguradora = "HDI"
                                break
                            if any(keyword in line for keyword in ["Companhia", "Tokio Marine", "Azul", "HDI SEGUROS S.A.", "Yelum", "Empresa:", "Suhai", "Bradesco Seguros", "SEGURADO", "Bradesco Auto"]):
                                seguradora = line.strip()
                                break
                            # Aqui estamos assumindo que a data de pagamento é uma linha que contém uma data (ajuste conforme necessário)
                            if "Data de Pagamento" in line:
                                data_pagamento = line.split(":")[1].strip()  # Extrair a data após "Data de Pagamento"

                    if seguradora is None:
                        seguradora = "Desconhecida"
                    
                    # Verifique se a data de pagamento foi extraída corretamente
                    if not data_pagamento:
                        data_pagamento = "Desconhecida"

                    for page in pdf.pages:
                        table = page.extract_table()
                        if table:
                            for row in table[1:]:
                                segurado = {}
                                if len(row) == 7:
                                    segurado = {
                                        'segurado': row[0],
                                        'apolice': row[1] if row[1] else 'N/A',
                                        'parcela': row[2] if row[2] else 'N/A',
                                        'comissao': row[3],
                                        'premio_liquido': row[4] if row[4] else "nothing",
                                        'total': row[5],
                                        'data_pagamento': data_pagamento,  # Adicionar a data de pagamento aqui
                                        'vendedores': []
                                    }
                                elif len(row) == 12:
                                    segurado = {
                                        'segurado': row[7],
                                        'apolice': row[4] if row[4] else 'N/A',
                                        'comissao': row[10],
                                        'premio_liquido': row[9],
                                        'total': row[9],
                                        'parcela': row[8],
                                        'data_pagamento': data_pagamento,  # Adicionar a data de pagamento aqui
                                        'vendedores': []
                                    }
                                elif len(row) == 14:
                                    if seguradora == "Empresa: PORTO SEGURO CIA DE SEGUROS GERAIS":
                                        segurado = {
                                            'segurado': row[0],
                                            'apolice': row[4] if row[4] else 'N/A',
                                            'comissao': row[12],
                                            'premio_liquido': row[10],
                                            'total': row[10],
                                            'parcela': 'N/A',
                                            'data_pagamento': data_pagamento,  # Adicionar a data de pagamento aqui
                                            'vendedores': []
                                        }
                                    elif seguradora == "Tokio Marine":
                                        segurado = {
                                            'segurado': row[1],
                                            'apolice': row[2],
                                            'comissao': row[11],
                                            'premio_liquido': row[9],
                                            'total': row[10],
                                            'parcela': row[8],
                                            'data_pagamento': data_pagamento,  # Adicionar a data de pagamento aqui
                                            'vendedores': []
                                        }
                                elif len(row) == 6:  # Novo formato
                                    segurado = {
                                        'segurado': row[0],
                                        'apolice': row[1] if row[1] else 'N/A',
                                        'parcela': row[2],
                                        'comissao': row[3],
                                        'premio_liquido': row[4],
                                        'total': row[5],
                                        'data_pagamento': data_pagamento,  # Adicionar a data de pagamento aqui
                                        'vendedores': []
                                    }
                                else:
                                    print(f"Tabela com formato desconhecido: {row}")
                                    continue

                                # Adicionar vendedores com base no nome do segurado
                                vendedores = get_vendedores_by_cliente(segurado['segurado'])
                                segurado['vendedores'] = vendedores

                                if seguradora not in segurados:
                                    segurados[seguradora] = []
                                segurados[seguradora].append(segurado)

            finally:
                temp_file.close()
                os.remove(temp_file.name)

        # Armazenar os dados na coleção com a data de pagamento e data de upload
        collection.insert_one({
            'data_upload': data_atual,
            'seguros': segurados
        })

        return jsonify({'seguros': segurados})

    except Exception as e:
        return jsonify({'error': f'Erro ao ler o PDF: {e}'})

# Rota para consultar por data de pagamento
@app.route('/consultar_por_data', methods=['GET'])
def consultar_por_data():
    data_pagamento = request.args.get('data_pagamento')
    if not data_pagamento:
        return jsonify({'error': 'Data de pagamento não fornecida'}), 400

    # Buscar no banco de dados com base na data de pagamento
    resultado = collection.find_one({'seguros.data_pagamento': data_pagamento})
    if not resultado:
        return jsonify({'error': 'Nenhum dado encontrado para essa data de pagamento'}), 404

    return jsonify({'seguros': resultado['seguros']})

if __name__ == '__main__':
    app.run(debug=True)

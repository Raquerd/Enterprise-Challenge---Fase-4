from flask import Flask, request, jsonify, datetime, oracledb

def registrar_no_banco(temperatura, vibracao, corrente, id_sistema):
    """Função para inserir os dados recebidos no banco de dados."""
    try:
        conn = oracledb.connect(user="rm562274", password="090402", dsn='oracle.fiap.com.br:1521/ORCL')
        cursor = conn.cursor()
        
        timestamp = datetime.datetime.now().isoformat()
        
        # Query SQL para inserir uma nova leitura
        query = "INSERT INTO FT_REG_SENSORES (TIMESTAMP_REGISTRO, VL_TEMPERATURA, VL_VIBRACAO, VL_CORRENTE, ID_SISTEMA) VALUES (:1, :2, :3, :4, :5)"
        cursor.execute(query, (timestamp, temperatura, vibracao, corrente, id_sistema))
        
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        print(f"Erro ao inserir no banco de dados: {e}")
        return False


app = Flask(__name__)

@app.route('/read', methods=['POST'])
def receber_leitura():
    '''Endpoint para receber dados dos sensores via POST.'''
    print("Recebendo dados do ESP32...")

    data = request.get_json()

    if not data:
        return jsonify({"status": "erro", "mensagem": "Nenhum dado recebido"}), 400
    
    temp = data.get('tempC')
    current = data.get('currentA')
    vibracao = data.get('vibracao_aprox')
    id_sistema = data.get('id_sistema')
    # acc_X = data.get('accX_g')
    # acc_Y = data.get('accY_g')
    # acc_Z = data.get('accZ_g')

    if temp is None or current is None or vibracao is None or id_sistema is None:
        return jsonify({"status": "erro", "mensagem": "Dados incompletos"}), 400

    
    print(f"Dados recebidos do sistema de id {id_sistema}: Temp={temp}, Corrente={current}, Vibração={vibracao}")


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
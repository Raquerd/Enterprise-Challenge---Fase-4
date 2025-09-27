from flask import Flask, request, jsonify

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
    acc_X = data.get('accX_g')
    acc_Y = data.get('accY_g')
    acc_Z = data.get('accZ_g')

    if temp is None or current is None or acc_X is None or acc_Y is None or acc_Z is None:
        return jsonify({"status": "erro", "mensagem": "Dados incompletos"}), 400
    
    print(f"Dados recebidos: Temp={temp}, Corrente={current}, Aceleração Eixo X={acc_X}, Aceleração Eixo Y={acc_Y}, Aceleração Eixo Z={acc_Z}")

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
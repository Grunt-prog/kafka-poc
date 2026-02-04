from flask import Flask, request, jsonify
from kafka_producer import send_to_kafka

app = Flask(__name__)

@app.route('/send', methods=['POST'])
def send_message():
    data = request.json
    if not data:    
        return jsonify({"error": "Missing JSON"}), 400

    send_to_kafka('flask-topic', data)
    return jsonify({"status": "sent"}), 200

if __name__ == '__main__':
    app.run(debug=True)

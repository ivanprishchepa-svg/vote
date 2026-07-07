import os
import redis
from flask import Flask, render_template, request

app = Flask(__name__)

# Подключение к Redis
redis_url = os.environ.get('REDIS_URL', 'redis://redis:6379')
r = redis.Redis.from_url(redis_url, decode_responses=True)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/vote', methods=['POST'])
def vote():
    option = request.form.get('option')
    if option:
        # Отправляем голос в список redis
        r.rpush('votes', option)
        return '', 204
    return 'Bad request', 400

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=True)

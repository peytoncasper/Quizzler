from flask import Flask, request, jsonify
from cassandra.cluster import Cluster
from cassandra.query import dict_factory
from flask.ext.cors import CORS
import uuid

try: import simplejson as json
except ImportError: import json


app = Flask(__name__)
CORS(app)

session = None

@app.route('/')
def hello_world():
    return 'Hello World!'

@app.route('/add_question')
def add_question():
    if not request.args.get('question') or not request.args.get('category') or not request.args.get('answer'):
        return jsonify({'error': 'question-text, answer, and category-name are required'})
    insert_question = session.prepare('''INSERT INTO quizzler.question
          (question_id, question, category_name, quizzler_percent)
    VALUES
          (?,?,?,?)''')
    insert_answer = session.prepare('''INSERT INTO quizzler.answers
           (answer_id, question_id, answer)
    VALUES
           (?,?,?)''')
    question = {
        'question_id': uuid.uuid1(),
        'question': request.args.get('question'),
        'category_name': request.args.get('category'),
        'quizzler_percent': 0
    }
    session.execute(insert_question.bind(question))
    answer = {
        'answer_id': uuid.uuid1(),
        'question_id': question["question_id"],
        'answer': request.args.get('answer'),
    }
    session.execute(insert_answer.bind(answer))
    return jsonify({"question_id": question["question_id"]})

@app.route('/get_questions')
def get_questions():
    get_questions = session.prepare('''SELECT * FROM quizzler.question''')
    questions = session.execute(get_questions)

    return jsonify(questions = questions.current_rows)


def init_cassandra():
    global session

    print("Connecting to Cassandra")
    cluster = Cluster()
    session = cluster.connect("quizzler")
    session.row_factory = dict_factory
    print("Connected to Cassandra")

if __name__ == '__main__':
    init_cassandra()
    app.run(host='0.0.0.0')


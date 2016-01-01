from flask import Flask
from cassandra.cluster import Cluster

app = Flask(__name__)

cluster,session = None

@app.route('/')
def hello_world():
    return 'Hello World!'

def 

def init_cassandra():
    global cluster, session

    print("Connecting to Cassandra")
    cluster = Cluster()
    session = cluster.connect("quizzler")
    print("Connected to Cassandra")

if __name__ == '__main__':
    init_cassandra()
    app.run()


import json
import random


def connectDHT(server):
    # Connect to server
    5


def put(connection, key, value):
    id_set = connection.get_id_set()
    id = random.sample(id_set,1)
    connection.send_request()


def main():
    connection = connectDHT("127.0.0.1")
    put(connection, 5, 100)



if __name__ == "__main__":
    main()

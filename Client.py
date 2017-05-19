import json
import random
import btpeer


def main():
    c = Client()

    while True:
        cmd = raw_input("COMMAND: ")
        parts = cmd.split(" ")
        if parts[0] == 'PUT':
            print(int(parts[1]))
            print(int(parts[2]))
            c.put(int(parts[1]), int(parts[2]))
        elif parts[0] == 'GET':
            print('Value is ' + str(c.get(int(parts[1]))))


class Client:
    def __init__(self):
        self.server = '207.154.219.184'
        self.port = '2222'
        self.client = btpeer.BTPeer(0, 2224)

        self.client.addpeer('server', self.server, self.port)
        self.get_id_set()

    def get_id_set(self):
        response = self.client.sendtopeer('server', "PSET", "")
        print ("ID SET: %s" % response)
        return json.loads(response[0][1])

    def put(self, key, value):
        id_set = self.get_id_set()
        index = random.sample(id_set, 1)[0]

        (ip, port) = index

        response = self.client.connectandsend(ip, port, "PUTX", json.dumps({'key': key, 'value': value, 'check': False}))
        print(response)
        return json.loads(response[0][1])

    def get(self, key):
        id_set = self.get_id_set()
        index = random.sample(id_set, 1)[0]

        (ip, port) = index

        response = self.client.connectandsend(ip, port, "GETX", json.dumps({'key': key, 'check': False}))
        return json.loads(response[0][1])['value']

    def contains(self,key):
        id_set = self.get_id_set()
        index = random.sample(id_set, 1)

        (ip, port) = id_set(index)

        response = self.client.connectandsend(ip, port, "CONT", key)
        return json.loads(response[0][1])

    def remove(self,key):
        id_set = self.get_id_set()
        index = random.sample(id_set, 1)

        (ip, port) = id_set(index)

        response = self.client.connectandsend(ip, port, "RMVX", key)
        return json.loads(response[0][1])


if __name__ == "__main__":
    main()

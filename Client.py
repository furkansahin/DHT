import json
import random
import btpeer


def main():
    Client()


class Client:
    def __init__(self):
        self.server = '127.0.0.1'
        self.port = '2222'
        self.client = btpeer.BTPeer(0, 2224)

        self.client.addpeer('server', self.server, self.port)

    def getIdSet(self):
        response = self.client.sendtopeer('server', "PSET", "")
        print ("ID SET: %s" % response)
        return json.loads(response)

    def put(self, key, value):
        id_set = self.getIdSet()
        index = random.sample(id_set, 1)

        (ip, port) = id_set(index)

        response = self.client.connectandsend(ip, port, "PUTX", key + "///" + value)
        return json.loads(response)

    def get(self, key):
        id_set = self.getIdSet()
        index = random.sample(id_set, 1)

        (ip, port) = id_set(index)

        response = self.client.connectandsend(ip, port, "GETX", key)
        return json.loads(response)

    def contains(self,key):
        id_set = self.getIdSet()
        index = random.sample(id_set, 1)

        (ip, port) = id_set(index)

        response = self.client.connectandsend(ip, port, "CONT", key)
        return json.loads(response)

    def remove(self,key):
        id_set = self.getIdSet()
        index = random.sample(id_set, 1)

        (ip, port) = id_set(index)

        response = self.client.connectandsend(ip, port, "RMVX", key)
        return json.loads(response)


if __name__ == "__main__":
    main()

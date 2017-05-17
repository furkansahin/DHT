import json
import random
import btpeer


def main():
    c = Client()
    c.put(15,5)


class Client:
    def __init__(self):
        self.server = '207.154.219.184'
        self.port = '2222'
        self.client = btpeer.BTPeer(0, 2224)

        self.client.addpeer('server', self.server, self.port)

        self.getIdSet()


    def getIdSet(self):
        response = self.client.sendtopeer('server', "PSET", "")
        print ("ID SET: %s" % response)
        return json.loads(response[0][1])

    def put(self, key, value):
        id_set = self.getIdSet()
        index = random.sample(id_set, 1)[0]

        (ip, port) = (index[0], index[1])

        response = self.client.connectandsend(ip, port, "PUTX", json.dumps({'key': key, 'value': value, 'check': False}))
        print(response)
        return json.loads(response[0][1])

    def get(self, key):
        id_set = self.getIdSet()
        index = random.sample(id_set, 1)

        (ip, port) = id_set(index)

        response = self.client.connectandsend(ip, port, "GETX", key)
        return json.loads(response[0][1])

    def contains(self,key):
        id_set = self.getIdSet()
        index = random.sample(id_set, 1)

        (ip, port) = id_set(index)

        response = self.client.connectandsend(ip, port, "CONT", key)
        return json.loads(response[0][1])

    def remove(self,key):
        id_set = self.getIdSet()
        index = random.sample(id_set, 1)

        (ip, port) = id_set(index)

        response = self.client.connectandsend(ip, port, "RMVX", key)
        return json.loads(response[0][1])


if __name__ == "__main__":
    main()

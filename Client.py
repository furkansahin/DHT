import json
import random
import btpeer
import time


def main():
    c = Client()

    put_dict = dict()
    total_time_put = 0
    total_local_time_put = 0
    total_time_get = 0
    total_local_time_get = 0
    while True:
        cmd = raw_input("COMMAND: ")
        parts = cmd.split(" ")

        if parts[0] == 'PUT':
            for i in range(int(parts[1])):
                key = random.randint(0, 2 ** int(parts[2]))
                val = random.randint(0, 2 ** int(parts[2]))

                ts_local = time.time()
                put_dict[key] = val

                ts = time.time()

                c.put(key, val)
                tf = time.time()

                total_local_time_put = total_local_time_put + ts - ts_local
                total_time_put = total_time_put + tf - ts
        elif parts[0] == 'GET':

            for (key, val) in put_dict.items():
                ts = time.time()
                val = c.get(key)
                total_time_get += time.time() - ts
                if val == int(val):
                    print("SUCCESS")
                else:
                    print("FAILED!")
                    break
        elif parts[0] == 'CONTAINS':
            print('Value is ' + str(c.contains(int(parts[1]))))
        elif parts[0] == 'REMOVE':
            print('Value is ' + str(c.remove(int(parts[1]))))

        print("TOTAL Put time: " + str(total_time_put))
        print("TOTAL local Put time: " + str(total_local_time_put))
        print("TOTAL Get time: " + str(total_time_get))
        total_time_get = 0
        total_time_put = 0
        total_local_time_put = 0

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

        response = self.client.connectandsend(ip, port, "PUTX",
                                              json.dumps({'key': key, 'value': value, 'check': False}))
        print(response)
        return json.loads(response[0][1])

    def get(self, key):
        id_set = self.get_id_set()
        index = random.sample(id_set, 1)[0]

        (ip, port) = index

        response = self.client.connectandsend(ip, port, "GETX", json.dumps({'key': key, 'check': False}))
        return json.loads(response[0][1])

    def contains(self, key):
        id_set = self.get_id_set()
        index = random.sample(id_set, 1)[0]

        (ip, port) = index

        response = self.client.connectandsend(ip, port, "CONT", json.dumps({'key': key, 'check': False}))
        return json.loads(response[0][1])

    def remove(self, key):
        id_set = self.get_id_set()
        index = random.sample(id_set, 1)[0]

        (ip, port) = index

        response = self.client.connectandsend(ip, port, "RMVX", json.dumps({'key': key, 'check': False}))
        return json.loads(response[0][1])

if __name__ == "__main__":
    main()

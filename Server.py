import json
import hashlib
import thread
import btpeer


def main():
    Server(40)


class Server:
    def __init__(self, circle_size):
        print "Called"
        self.nodes = dict()
        self.circle_size = circle_size
        self.node = btpeer.BTPeer(0,2222);

        print self.node.serverhost
        self.node.addhandler('SWRQ', self.request_handler)
        self.node.mainloop()

    def request_handler(self,conn, msg):
        ip = conn.ip
        port = conn.port
        request = "incoming request:%s" % msg
        print request
        hash = self.calculate_hash(ip)
        message = json.dumps({"ip": ip, "id": hash, "idDictionary": self.node.getpeerids(), "m": self.circle_size})

        print "Adding"
        self.node.addpeer(ip,ip,port)
        print "Peers%s" % self.node.getpeerids()

        conn.senddata("SWRQ",message)

    def calculate_hash(self, definition):
        hex_hash = hashlib.sha1(definition).hexdigest()
        key = int(hex_hash, 16) % 2 ** self.circle_size
        return key

if __name__ == "__main__":
    main()
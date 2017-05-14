import Node
import json
from pyp2p.net import Net
import time

def main():
    # ipAddr = sys.argv(0)
    # portNum = sys.argv(1)
    #
    # jsonObj = json.loads(connect_to_server(ipAddress=ipAddr, portNum=portNum))
    node = Node.Node()
    finger_table = node.create_finger_table()
    node.incoming_query("{\"ip\": \"127.0.0.1\", \"port\": \"2222\", \"key\": \"44\"}")


if __name__ == "__main__":
    main()
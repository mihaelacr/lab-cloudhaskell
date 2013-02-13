import SocketServer
import getWorkingLabMachines

class Handler(SocketServer.BaseRequestHandler):
  def handle(self):
    print self.request
    self.request.sendall("\n".join(getWorkingLabMachines.getWorkingMachines()))

web_server = SocketServer.TCPServer(("localhost", 8081), Handler)
web_server.serve_forever()

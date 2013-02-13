import SocketServer
import getWorkingLabMachines

class Handler(SocketServer.BaseRequestHandler):

  def handle(self):
    print self.request
    machines = "\n".join(getWorkingLabMachines.getWorkingMachines())
    self.request.sendall(machines)

web_server = SocketServer.TCPServer(("localhost", 8081), Handler)
web_server.serve_forever()

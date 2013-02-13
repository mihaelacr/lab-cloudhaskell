import subprocess
import re


def getAllMachines():
  condor_output = subprocess.check_output("condor_status", shell=True)
  hostnames = []
  for line in condor_output.split("\n"):
    if re.search("doc", line):
      hostname = line.split()[0]
      hostnames += [re.split("\W+", hostname)[0]]
  return hostnames


def getWorningMachines(hostnames=None, extension=None):
  if not hostnames:
    hostnames = getAllMachines()
  if not extension:
    extension = ".doc.ic.ac.uk"
  workingHostames = []
  for h in hostnames:
    h += extension
    try:
      subprocess.check_output(["ping", "-c", "1", h], stderr=subprocess.PIPE)
    except subprocess.CalledProcessError, e:
      pass
    else:
      workingHostames += [h]
      print h
  return workingHostames

if __name__ == '__main__':
  print getWorningMachines()

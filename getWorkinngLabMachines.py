import subprocess
import re

condor_output = subprocess.check_output("condor_status", shell=True)
hostnames = []
EXTENSION = ".doc.ic.ac.uk"


for line in condor_output.split("\n"):
  if re.search("doc", line):
    hostname = line.split()[0]
    hostnames += [re.split("\W+", hostname)[0]]

for h in hostnames:
  h += EXTENSION
  try:
    subprocess.check_output(["ping", "-c", "1", h], stderr=subprocess.PIPE)
  except subprocess.CalledProcessError, e:
    pass
  else:
    print h

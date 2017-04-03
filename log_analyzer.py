import sys, getopt
import re

logrx = re.compile('([^ ]+\s+[0-9][0-9]? [0-9][0-9]:[0-9][0-9]:[0-9][0-9])(.*)$')

def main(argv):
   inputfile = ''
   outputfile = ''
   try:
      opts, args = getopt.getopt(argv,"q",["n", "hostname"])
   except getopt.GetoptError:
      print 'log_analyzer.py -q  <questionnumber> <host1> <host2> <host3> <hostN>'
      sys.exit(2)

   for opt, arg in opts:
      if opt != '-q':
	 print 'log_analyzer.py -q  <questionnumber> <host1> <host2> <host3> <hostN>'
	 sys.exit(2)
       

   
   hosts = []
   for i in range(len(args)):
    if  i == 0:
	number=int(args[0])
    else:
      hosts.append(args[i])

   
   if number == 1:
	question1(hosts)


def showResult(x):
	print x

def normalizeWords(text):
	m = logrx.match(text)
	return m.group(2).split()

def question1(hosts):
	lines = sc.textFile("file:///var/log/syslog")
	
	fields =lines.flatMap(lambda x:normalizeWords(x))
	fltr= fields.filter(lambda x: x in hosts)


	pairs=fltr.map(lambda x:(x,1))
	counts=pairs.reduceByKey(lambda x,y: x+y)
	showResult("* Q1: line counts")
	counts.foreach(lambda x: showResult("	+ " + str(x[0])+": "+str(x[1])))    

def question2(hosts):
	lines = sc.textFile("file:///var/log/syslog")
	
	fields =lines.flatMap(lambda x:normalizeWords(x))
	fltr= fields.filter(lambda x: x in hosts )


	pairs=fltr.map(lambda x:(x,1))
	counts=pairs.reduceByKey(lambda x,y: x+y)
	showResult("* Q1: line counts")
	counts.foreach(lambda x: showResult("	+ " + str(x[0])+": "+str(x[1])))         
  
if __name__ == "__main__":
   main(sys.argv[1:]) 









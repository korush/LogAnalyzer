import sys, getopt
import re

filePath = 'a/log'

re1='((?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Sept|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?))'	# Month 1
re2='.*?'	# Non-greedy match on filler
re3='((?:(?:[0-2]?\\d{1})|(?:[3][01]{1})))(?![\\d])'	# Day 1
re4='.*?'	# Non-greedy match on filler
re5='((?:(?:[0-1][0-9])|(?:[2][0-3])|(?:[0-9])):(?:[0-5][0-9])(?::[0-5][0-9])?(?:\\s?(?:am|AM|pm|PM))?)'	# HourMinuteSec 1
re6='.*?'	# Non-greedy match on filler
re7='((?:[a-z][a-z]+))'	# Word 1

LINUX_LOG_PATTERN = re1+re2+re3+re4+re5+re6+re7

def write(x):
	print x


def splitLogQ1(text, pattern):
		logrx = re.compile(pattern,re.IGNORECASE|re.DOTALL)
		m = logrx.match(text)
		
		if m is None:
		  return None
		
		return (m.group(4), 1)

def question1(hosts):
	
	pattern = LINUX_LOG_PATTERN +  '([\s\S]+[\w\W]+[\d\D])'

	lines = sc.textFile(filePath)
	logs = lines.map(lambda l: splitLogQ1(l, pattern))
	fltr = logs.filter(lambda x: x is not None and x[0] in hosts)
	counts=fltr.reduceByKey(lambda x,y: x+y).sortByKey()
	
	counts.foreach(lambda x: write("	+ " + str(x[0])+": "+str(x[1])))   
	


def splitLogQ2(text, pattern):
		logrx = re.compile(pattern,re.IGNORECASE|re.DOTALL)
		m = logrx.match(text)
		
		if m is None:
		  return None
	
		return (m.group(4), m.group(9))
	

def question2(hosts):

	pattern = LINUX_LOG_PATTERN +  '([\s\S]+[\w\W]+[\d\D])' + '(Started Session )' + '([0-9]+)' + '( of user )' + '([\w\W]+[\d\D]+)' + '(.)'

	lines = sc.textFile(filePath)
	logs = lines.map(lambda l: splitLogQ2(l, pattern))
	fltr = logs.filter(lambda x: x is not None and x[0] in hosts and x[1] in 'achille')
	pairs = fltr.map(lambda x: (x[0],1))
	counts= pairs.reduceByKey(lambda x,y: x + y).sortByKey()
	counts.foreach(lambda x: write("	+ " + str(x[0])+": "+str(x[1])))   
	

def question3(hosts):

	pattern = LINUX_LOG_PATTERN +  '([\s\S]+[\w\W]+[\d\D])' + '(Started Session )' + '([0-9]+)' + '( of user )' + '([\w\W]+[\d\D]+)' + '(.)'

	lines = sc.textFile(filePath)
	logs = lines.map(lambda l: splitLogQ2(l, pattern))
	fltr = logs.filter(lambda x: x is not None and x[0] in hosts)
	counts= fltr.reduceByKey(lambda x,y: x + ',' + y)
	counts.foreach(lambda x: write("	+ " + str(x[0])+": ["+str(x[1]) + "]")) 




def question4(hosts):

	pattern = LINUX_LOG_PATTERN +  '([\s\S]+[\w\W]+[\d\D])' + '(Started Session )' + '([0-9]+)' + '( of user )' + '([\w\W]+[\d\D]+)' + '(.)'

	lines = sc.textFile(filePath)
	logs = lines.map(lambda l: splitLogQ2(l, pattern))
	fltr = logs.filter(lambda x: x is not None and x[0] in hosts)
	pairs = fltr.map(lambda x: ((x[0],x[1]), 1))
	reduced = pairs.reduceByKey(lambda x, y: x + y)
	results = reduced.map(lambda x: (x[0][0], "(" + x[0][1] + "," +  str(x[1]) + ")")) #"(" + x[0][1] "," + str(x[1]) + ")"))
	counts= results.reduceByKey(lambda x,y: x + ',' + y)
	counts.foreach(lambda x: write("	+ " + str(x[0])+": ["+str(x[1]) + "]")) 

def splitLogQ5(text, pattern):
		logrx = re.compile(pattern,re.IGNORECASE|re.DOTALL)
		m = logrx.match(text)
		
		if m is None:
		  return None
		
		return (m.group(4), m.group(5))

def question5(hosts):
	
	pattern = LINUX_LOG_PATTERN + '([\s\S]+[\w\W]+[\d\D]+)'

	lines = sc.textFile(filePath)
	logs = lines.map(lambda l: splitLogQ5(l, pattern))
	fltr = logs.filter(lambda x: x is not None and x[0] in hosts and 'error' in x[1])
	
	pairs = fltr.map(lambda x: (x[0], 1))
	counts=pairs.reduceByKey(lambda x,y: x+y).sortByKey()
	
	counts.foreach(lambda x: write("	+ " + str(x[0])+": "+str(x[1])))   
	
	

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
   elif number == 2:
	question2(hosts)
   elif number == 3:
	question3(hosts)
   elif number == 4:
	question4(hosts)
   elif number == 5:
	question5(hosts)



if __name__ == "__main__":
   main(sys.argv[1:]) 









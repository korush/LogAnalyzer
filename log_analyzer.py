import sys, getopt
import re
import os
import shutil



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

def question1(paths, hosts):
	
	pattern = LINUX_LOG_PATTERN +  '([\s\S]+[\w\W]+[\d\D])'

	lines = sc.textFile(','.join(paths))
	logs = lines.map(lambda l: splitLogQ1(l, pattern))
	fltr = logs.filter(lambda x: x is not None and x[0] in hosts)
	counts=fltr.reduceByKey(lambda x,y: x+y).sortByKey()
	write("* Q1: line counts")
	counts.foreach(lambda x: write("	+ " + str(x[0])+": "+str(x[1])))   
	


def splitLogQ2(text, pattern):
		logrx = re.compile(pattern,re.IGNORECASE|re.DOTALL)
		m = logrx.match(text)
		
		if m is None:
		  return None
	
		return (m.group(4), m.group(9))
	

def question2(paths, hosts):

	pattern = LINUX_LOG_PATTERN +  '([\s\S]+[\w\W]+[\d\D])' + '(Started Session )' + '([0-9]+)' + '( of user )' + '([\w\W]+[\d\D]+)' + '(.)'

	lines = sc.textFile(','.join(paths))
	logs = lines.map(lambda l: splitLogQ2(l, pattern))
	fltr = logs.filter(lambda x: x is not None and x[0] in hosts and x[1] in 'achille')
	pairs = fltr.map(lambda x: (x[0],1))
	counts= pairs.reduceByKey(lambda x,y: x + y).sortByKey()
	write("* Q2: sessions of user achille")	
	counts.foreach(lambda x: write("	+ " + str(x[0])+": "+str(x[1])))   
	

def question3(paths, hosts):

	pattern = LINUX_LOG_PATTERN +  '([\s\S]+[\w\W]+[\d\D])' + '(Started Session )' + '([0-9]+)' + '( of user )' + '([\w\W]+[\d\D]+)' + '(.)'

	lines = sc.textFile(','.join(paths))
	logs = lines.map(lambda l: splitLogQ2(l, pattern))
	fltr = logs.filter(lambda x: x is not None and x[0] in hosts).distinct()
	counts= fltr.reduceByKey(lambda x,y: x + ',' + y)
	write("* Q3: unique user names")	
	counts.foreach(lambda x: write("	+ " + str(x[0])+": ["+str(x[1]) + "]")) 




def question4(paths, hosts):

	pattern = LINUX_LOG_PATTERN +  '([\s\S]+[\w\W]+[\d\D])' + '(Started Session )' + '([0-9]+)' + '( of user )' + '([\w\W]+[\d\D]+)' + '(.)'

	lines = sc.textFile(','.join(paths))
	logs = lines.map(lambda l: splitLogQ2(l, pattern))
	fltr = logs.filter(lambda x: x is not None and x[0] in hosts)
	pairs = fltr.map(lambda x: ((x[0],x[1]), 1))
	reduced = pairs.reduceByKey(lambda x, y: x + y)
	results = reduced.map(lambda x: (x[0][0], "(" + x[0][1] + "," +  str(x[1]) + ")")) 
	counts= results.reduceByKey(lambda x,y: x + ',' + y).sortByKey()
	write("* Q4: sessions per user")
	counts.foreach(lambda x: write("	+ " + str(x[0])+": ["+str(x[1]) + "]")) 

def splitLogQ5(text, pattern):
		logrx = re.compile(pattern,re.IGNORECASE|re.DOTALL)
		m = logrx.match(text)
		
		if m is None:
		  return None
		
		return (m.group(4), m.group(5))

def question5(paths, hosts):
	
	pattern = LINUX_LOG_PATTERN + '([\s\S]+[\w\W]+[\d\D]+)'

	lines = sc.textFile(','.join(paths))
	logs = lines.map(lambda l: splitLogQ5(l, pattern))
	fltr = logs.filter(lambda x: x is not None and x[0] in hosts and 'error' in x[1])
	
	pairs = fltr.map(lambda x: (x[0], 1))
	counts=pairs.reduceByKey(lambda x,y: x+y).sortByKey()
	write("* Q5: number of errors")
	counts.foreach(lambda x: write("	+ " + str(x[0])+": "+str(x[1])))   

def PrintQuestion6(key, pairs):
	write(" +" +  key)  
	
 	pairs.sort(key = lambda x: -x[1])
	for x in pairs[:5]:
		write("  -(" + str(x[1]) + ', ' + x[0] + ')')

	write(' ')
	
def question6(paths, hosts):
	
	pattern = LINUX_LOG_PATTERN + '([\s\S]+[\w\W]+[\d\D]+)'

	lines = sc.textFile(','.join(paths))
	logs = lines.map(lambda l: splitLogQ5(l, pattern))
	fltr = logs.filter(lambda x: x is not None and x[0] in hosts and 'error' in x[1])
	
	pairs = fltr.map(lambda x: ((x[0], x[1]), 1))
	reduced=pairs.reduceByKey(lambda x,y: x+y)

	counts = reduced.map(lambda x: (x[0][0],  (x[0][1], x[1]))).groupByKey().mapValues(list)
	
	write("* Q6: 5 most frequent error messages")
	counts.foreach(lambda x: PrintQuestion6(x[0], x[1]))


def splitLogQ7(text, pattern):
		logrx = re.compile(pattern,re.IGNORECASE|re.DOTALL)
		m = logrx.match(text)
		
		if m is None:
		  return None
		
		return (m.group(9), m.group(4))

def question7(paths, hosts):

	pattern = LINUX_LOG_PATTERN +  '([\s\S]+[\w\W]+[\d\D])' + '(Started Session )' + '([0-9]+)' + '( of user )' + '([\w\W]+[\d\D]+)' + '(.)'

	lines = sc.textFile(','.join(paths))
	logs = lines.map(lambda l: splitLogQ7(l, pattern))
	fltr = logs.filter(lambda x: x is not None and x[1] in hosts).distinct()
	pairs = fltr.map(lambda x: (x[0], 1))
	result = pairs.reduceByKey(lambda x,y: x + y)

	counts = result.filter(lambda x: x[1] == len(hosts))
	write("* Q7: users who started a session on both hosts, i.e., on exactly 2 hosts.")	
	counts.foreach(lambda x: write("	+ : "+ x[0])) 

def question8(paths, hosts):

	pattern = LINUX_LOG_PATTERN +  '([\s\S]+[\w\W]+[\d\D])' + '(Started Session )' + '([0-9]+)' + '( of user )' + '([\w\W]+[\d\D]+)' + '(.)'

	lines = sc.textFile(','.join(paths))
	logs = lines.map(lambda l: splitLogQ7(l, pattern))
	fltr = logs.filter(lambda x: x is not None and x[1] in hosts).distinct()
	pairs = fltr.map(lambda x: (x[0], (x[1],1)))
	
	result = pairs.reduceByKey(lambda x,y: (x[0], x[1]+ y[1]))

	counts = result.filter(lambda x: x[1][1] == 1)
	write("* Q8: users who started a session on exactly one host, with host name.")	
	counts.foreach(lambda x: write("	+ " + str(x[0])+": "+str(x[1][0]))) 




def sortUsers(pairs):
	users = []
 	pairs.sort(key = lambda x: x)
	i = 0
	for x in pairs:
		users.append((str(x), "user-" + str(i)))
		i+=1
	return users


def splitLogQ9(text, pattern):
		logrx = re.compile(pattern,re.IGNORECASE|re.DOTALL)
		m = logrx.match(text)
		
		if m is None:
		  return None
		
		return m.group(4)

def mapUsers(line, pairs):
	users = []
 	pairs.sort(key = lambda x: x)
	
	i = 0
	for x in pairs:
		line = line.replace(str(x), "user-" + str(i))
		i+=1
	return line

def mapString(pairs):
	mapedUsers = []
 	pairs.sort(key = lambda x: x)
	
	i = 0
	for x in pairs:
		mapedUsers.append('(' + str(x) +  ", user-" + str(i) + ')')
		i+=1
	return mapedUsers

def printQuestion9(key, mapStr):
	write("   + " +  key)  
	write("     User name mapping: " + str(mapStr))
	write("   Anonymized files: output/" + key + '-anonymized-10')


def question9(paths, hosts):

	patternHost = LINUX_LOG_PATTERN +  '([\s\S]+[\w\W]+[\d\D])'
	pattern =  patternHost + '(Started Session )' + '([0-9]+)' + '( of user )' + '([\w\W]+[\d\D]+)' + '(.)'

	lines = sc.textFile(','.join(paths))
	
	logs = lines.map(lambda l: splitLogQ2(l, pattern))
	fltr = logs.filter(lambda x: x is not None and x[0] in hosts).distinct()
	result = fltr.groupByKey().mapValues(list)

	ls = lines.map(lambda l: (splitLogQ9(l, patternHost), l))
	jl = ls.join(result)
	
	final = jl.map(lambda x: (x[0], mapUsers(x[1][0], x[1][1])))

	userMappers = result.map(lambda x: (x[0], mapString(x[1])))
	
	if os.path.exists("output"):
	    shutil.rmtree('output')

	for host in hosts:
		f = final.filter(lambda x: x[0] in host)
		f1 = f.map(lambda x: x[1])		
		filename = "output/" + host + '-anonymized-10'	
		f1.saveAsTextFile(filename)
	
	userMappers.foreach(lambda x: printQuestion9(x[0], x[1]))	

def main(argv):
   inputfile = ''
   outputfile = ''
   try:
      opts, args = getopt.getopt(argv,"q",["n", "hostname"])
   except getopt.GetoptError:
      write('log_analyzer.py -q  <questionnumber> <host1> <host2> <host3> <hostN>')
      sys.exit(2)

   for opt, arg in opts:
      if opt != '-q':
	 write('log_analyzer.py -q  <questionnumber> <host1> <host2> <host3> <hostN>')
	 sys.exit(2)
       
	
   
   hosts = []
   paths = []
   for i in range(len(args)):
    if  i == 0:
	number=int(args[0])
    else:
      if not os.path.exists(args[i]):
	    write('Directory ' + args[i] + ' does not exists!')
            sys.exit(1)
      paths.append(args[i])
      hosts.append(os.path.basename(os.path.normpath(args[i])))

   
   if number == 1:
	question1(paths, hosts)
   elif number == 2:
	question2(paths, hosts)
   elif number == 3:
	question3(paths, hosts)
   elif number == 4:
	question4(paths, hosts)
   elif number == 5:
	question5(paths, hosts)
   elif number == 6:
	question6(paths, hosts)
   elif number == 7:
	question7(paths, hosts)
   elif number == 8:
	question8(paths, hosts)
   elif number == 9:
	question9(paths, hosts)


if __name__ == "__main__":
   main(sys.argv[1:]) 









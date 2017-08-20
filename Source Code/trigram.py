import pyspark

import string
import re
import itertools

sc = pyspark.SparkContext()

lemma_map={}

text_file=sc.textFile("files")#Enter path of input file
file="/home/surabhi/Downloads/DIC/new_lemmatizer.csv"# enter path of lemmitization file




temp=open(file,'r')
lemma_lines=temp.readlines()
	
for l in lemma_lines:
	x = l.strip().split(",")
	y = [str(x[i]) for i in range(len(x)) if i!=0 and x[i]!="" and x[i]!="\n"]
	lemma_map[x[0]] = y
	

def removePunctuation(t):
 t=t.lower().strip();
 t = re.sub("\s+", " ", t)


 return t

def preprocess(x):
	x=removePunctuation(x)
	line_split=x.split(">",1)
	length_line_split=len(line_split)
	if length_line_split!=2:
		return[]

	location= line_split[0] + ">"
	line_split[1]=line_split[1].replace("j","i")
	line_split[1]=line_split[1].replace("v","u")
	line_split[1]="".join(c for c in line_split[1] if c not in ('!','.',':',';','*','@','~','#','$','%','"','?','/',','))
	word=line_split[1].split()
	length=len(word)
	#print("word",word)
	#print("len",length)
	map_values=[]
	#print("Location",line_split[0])
	#print("Words",line_split[1])
	
	
	word_pair=list()
	list_words=list()
	for i in range(0,length):
		
	 for j in range (0,length):

	  for k in range(0,length):
		
	   if i==j==k or i==j or i==k or j==k:
	    continue;
	   list_words.append(word[i])
	   list_words.append(word[j])
	   list_words.append(word[k])
	   word_pair.append(list_words)
	   list_words=list()
		 	#word_pair=list()
	#print(word_pair)

	
	
	for w in word_pair:
		#print("WORD",w)
		w1,w2,w3=w
		#print("word1",w1)
		#print("word2",w2)
		if w1 in lemma_map:
			w1=lemma_map[w1]
		else:
			w1=[w1]
		if w2 in lemma_map:
			w2=lemma_map[w2]
		else:
			w2=[w2]
		if w3 in lemma_map:
			w3=lemma_map[w3]
		else:
			w3=[w3]
		

		for x in w1:
			for y in w2:
				for z in w3:
					
				
				
				 map_values.append([','.join([x,y,z]),[location]])

	#print(map_values)
	return map_values

split_text_file=text_file.filter( lambda l: len(l.split(">"))!=1)
cooccur=split_text_file.flatMap(preprocess)
final_cooccur=cooccur.reduceByKey(lambda a,b:a+b)
final_cooccur.saveAsTextFile("/home/surabhi/Downloads/DIC/trigram_1")# enter path of output file


import MapReduce
import sys



mr = MapReduce.MapReduce()


def mapper(record):   
  name = record[0]
  i = record[1]    
  j = record[2]
  num = record[3]
  if (name== "a"):
    key = name + str(i) 
  elif (name == "b"):
    key = name + str(j)
  mr.emit_intermediate(key, record[1:])
    

def reducer(key, list_of_values):
  if (key[0] == "a"):
    for k in mr.intermediate:
      if k[0]== "b":
        total = 0
        for va in list_of_values:
          for vb in mr.intermediate[k]:
            if va[1]==vb[0] :
              total = total+va[-1]*vb[-1]
        
        a=int(key[1])
        b=int(k[1])
        mr.emit((a,b,total))
            
    


  
    
    
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
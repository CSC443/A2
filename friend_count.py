import MapReduce
import sys



mr = MapReduce.MapReduce()


def mapper(record):   
  key = record[0] 
  value = record[1] 
  
    
  mr.emit_intermediate(key, value)
  mr.emit_intermediate(value,value)

    

def reducer(key, list_of_values):
  total=0
  for v in list_of_values:
    if (key in mr.intermediate[v] and key!=v):
        total = total + 1
  mr.emit((key, total))  


  
    
    
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
import MapReduce
import sys



mr = MapReduce.MapReduce()


def mapper(record):   
  key = record[1]
  value = record   
  mr.emit_intermediate(key, value)
    

def reducer(key, list_of_values):
  
  order=[]
  line=[]
  if len(list_of_values) >= 2:
    for v in list_of_values:
      if (v[0]=="order"):
        order.append(v)
        
      elif (v[0]=="line_item"):
        line.append(v)
        
  
  
  for o in order:
    
    for l in line:
      item=o+l 
      mr.emit((item))
      
      



  
    
    
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
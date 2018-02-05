from jsonschema import validate
import time

schema = {
     "type" : "object",
     "properties" : {
         "price" : {"type" : "number"},
         "name" : {"type" : "string"},
    },
 }
t=time.time()
validate({"name" : "Eggs", "price" : 34.99}, schema)
t1=time.time()-t
print(t1)

# validate(
#      {"name" : "Eggs", "price" : "Invalid"}, schema
# )                                   # doctest
from pymongo import MongoClient
from neo4j import GraphDatabase
from tqdm import tqdm
from pprint import pprint




dburl = "mongodb://localhost:27017"
mongo = MongoClient(dburl)

db = mongo["kb2023"]
nimble_vendors = db["nimble_live_vendors"]
kb_vendors = db["vendors"]

nimble_vendors_list = list(nimble_vendors.find({},{"_id":0,"name":1,"address1":1,"city":1,"state":1,"zipCode":1,"country":1}))
kb_vendors_list = list(kb_vendors.find({},{"_id":0,"name":1,"address":1,"city":1,"state":1,"zip_code":1,"country":1}))

uri = "neo4j://localhost:7687"
userName = "jayaram"
password = "123456789"

driver = GraphDatabase.driver(uri=uri,  auth=(userName, password))

exclude_list =["_id","name","address","status_1099","node_created_by","last_updated","geolocation"]

class kb():
    def __init__(self) -> None:
        pass
    
    def normalizeStr(self,mystring):
        mystring = str(mystring)
        if mystring is None:
            return ''
        if type(mystring) == bool:
            return str(mystring)
        return mystring.replace('"', '').replace("'", "")
        
    def create_kbtoolvendors(self,v):
        query = f"MERGE (vg:VENDOR_GROUP{{name:\"{self.normalizeStr(v['name'])}\",address:\"{self.normalizeStr(v['address'])}\"}})\n ON CREATE SET"
        for key, value in v.items():
            if key in exclude_list:
                continue
            query += "\n vg."+key+" = \'"+self.normalizeStr(value)+"\',"
        query = query[:-1]
        print(query)
        with driver.session() as session:
            result = session.run(query)
        return (result)

    def jaccard_similarity(self,set1, set2):
        intersection = len(set1.intersection(set2))
        union = len(set1.union(set2))
        if union != 0:
            similarity = intersection / union
            return similarity
        else: return None
    
    def text_to_set(self,str):
        str = "".join(ch for ch in str if ch.isalnum() or ch == " ")
        r = set(str.lower().split())
        return r

            
    def find_matching_kbv(self,v):
        vdata = v.dict()
        set1 = set()
        for key,value in vdata.items():
            value = str(value)
            if value == "string" or value == "":
                continue
            set1 = set1.union(self.text_to_set(value))
        # print(set1)
        res = []
        simlist = []

        for v in kb_vendors_list:
            set2 =set()
            for key,value in v.items():
                value = str(value)
                if value == " " or value == "" :
                    continue
                set2 = set2.union(self.text_to_set(value))
            sim = self.jaccard_similarity(set1,set2)
            simlist.append(sim)
            if sim >= 0.5:
                # print("----------------------")
                # pprint(vdata)
                # print("<><><><><><><><><><><><><><><><><><><><><><><>")
                # pprint(v)
                # print("----------------------")
                # print(set1)
                # print("<><><><><><><><><><><><><><><><><><><><><><><>")
                # print(set2)
                res.append(v)
        #print(len(simlist),max(simlist))        
        return res
                

# for v in tqdm(kb_vendors_list):
#     # print(v.keys())
#     KB.create_kbtoolvendors(v)
    

 

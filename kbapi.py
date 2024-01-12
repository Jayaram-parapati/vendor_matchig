from fastapi import Body,FastAPI,HTTPException
from fastapi.responses import JSONResponse, RedirectResponse
from pydantic import BaseModel
from matching_nbv_kbv import kb
kbapi = kb()


app = FastAPI()




class kbvbasemodel(BaseModel):
    name:str = "THE SHERWIN-WILLIAM CO"
    address:str = "3119 ARDEN WAY"
    city:str = "SACRAMENTO"
    state:str = "California"
    zip_code:str = "95825-2094"
    country:str = "UNITED STATES OF AMERICA"
    phone1:str|None =None 
    website:str|None =None 
    phone2:str|None =None 
    email:str|None =None 
    bussiness_name:str|None =None 
    type:str|None =None 
    
    
    
    
@app.post("/findkbv")
def createvg(vdata:kbvbasemodel = Body(...)):
    res = kbapi.find_matching_kbv(vdata)
    return res
    

from main import Table, Col



def mainTest():
    """tests createTAble query and createRow query 
    \nnone unit"""
    users = Table(Col("username", "string"), Col("password", "string"))
    users.createRow(("username", "password"), ("shmuli", "keller"))
    assert users.getFirst(["password"], where=["username"], value=["shmuli"]) == "shmuli"

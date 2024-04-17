
"""goal: create a simple database system

createTAble query: users = Table(Col("username", "string"), Col("password", "string"))
createRow query: users.createRow(("username", "password"), ("shmuli", "keller"))
get query: users.getFirst(["password"], where=["username"], value=["shmuli"])
get query: users.update(field="password", to="ngix", where="username", equals="shmuli")


have indexing using a prefix tree that leads a the users name to an id wich can then get a binary search on it for faster lookup

"""

from collections import defaultdict



returnsDefaultDict = lambda: defaultdict(returnsDefaultDict)


tables = {}

class Col:
    def __init__(self, colName: str, colType: type):
        self.colName = colName
        self.colType = colType

    def method_name(self, method_arguments):
        pass



class Table:
    """
    attars: 
    self.colPositions
    self.coltypes
    self.tableContents
    self.trees (used for indexing)
    self.updating (used for stopping race conditions when doing concurrency)

    methods:
    createRow
    getAll
    getFirst
    updateFirst
    updateAll
    """
    def __init__(self, *cols: list[Col]):
        self.colPositions = {cols[i].colName: i for i in range(len(cols))}
        self.coltypes = {cols[i].colName: cols[i].colType for i in range(len(cols))}
        self.tableContents: list[list] = []
        self.trees = {col.colName: returnsDefaultDict() for col in cols}
        self.updating = []
        self.objectsInserted = 0

        def createRow(colNames: tuple, colValues: tuple):
            if len(colNames) != len(colValues):
                    raise ValueError("Number of column names does not match number of column values")
            if len(colNames) > len(self.colPositions):
                    raise ValueError("you have provided more names and values than there for each row in this table")
                
            newRow = list(None for _ in range(len(self.colPositions)))
            for i, colName in enumerate(colNames):
                if colName not in self.colPositions:
                    raise KeyError(f"Column {colName} not found in table")
                if not isinstance(colValues[i], self.coltypes[colName]):
                    raise TypeError(f"Column {colName} is not of type {self.colTypes[colName]}")
                positionOfCurrentValue = self.colPositions[colName]
                newRow[positionOfCurrentValue] = colValues[positionOfCurrentValue]

            self.tableContents.append(newRow)
            self.updating.append(False)

            tableLength = len(self.tableContents)
            for colName, colValue in zip(colNames, colValues):
                self.insert(colName, colValue, tableLength - 1)

            self.objectsInserted += 1

        self.createRow = createRow


    def getFirst(self, lookingFors: list[str]|None = None, 
                    wheres: list[str]|None = None, 
                    values: list[str]|None = None) -> list:
        
        
        """
        used to get the first row that matches the where clause"""
        if wheres and any(i not in self.colPositions for i in wheres):
            raise KeyError(f"Column not found in table")
        if wheres: wheres = [self.colPositions[where] for where in wheres]

        lookAtLater = []

        for index, row in enumerate(self.tableContents):
            if self.updating[index]:
                lookAtLater.append(index)
                continue
            if wheres and any(row[num] != values[i] for i, num in enumerate(wheres)):
                continue

            if not lookingFors:
                return row
            collumnsInLookingFors = filter(lambda i: i in lookingFors, self.colPositions.keys())
            return [row[self.colPositions[i]] for i in collumnsInLookingFors]
        
        for rowIndex in lookAtLater:
            if self.updating[rowIndex]:
                continue
            row = self.tableContents[rowIndex]
            if wheres and any(row[num] != values[i] for i, num in enumerate(wheres)):
                continue
            if not lookingFors:
                return row
            collumnsInLookingFors = filter(lambda i: row[i] in lookingFors, range(len(row)))
            return [row[i] for i in collumnsInLookingFors]

        return None
    
    def getAll(self, lookingFors: list[str]|None = None, 
                    wheres: list[str]|None = None, 
                    values: list[str]|None = None) -> list[list]:
        """used to get all rows that match the where clause"""

        if wheres and any(i not in self.colPositions for i in wheres):
            raise KeyError(f"Column not found in table")
        if wheres: wheres = [self.colPositions[where] for where in wheres]
        
        rows = []
        lookAtLater = []

        for index, row in enumerate(self.tableContents):
            if self.updating[index]:
                lookAtLater.append(index)
                continue
            if wheres and any(row[num] != values[i] for i, num in enumerate(wheres)):
                continue
            if not lookingFors:
                rows.append(row)
                continue
            collumnsInLookingFors = []
            for key, value in  self.colPositions.items():
                if key in lookingFors:
                    collumnsInLookingFors.append(value)
            rows.append([row[i] for i in collumnsInLookingFors])
            
        while lookAtLater:
            for index, rowIndex in enumerate(lookAtLater):
                row = self.tableContents[rowIndex]

                if self.updating[index]:
                    lookAtLater.append(index)
                    continue

                if wheres and any(row[num] != values[i] for i, num in enumerate(wheres)):
                    continue
                if not lookingFors:
                    rows.append(row)
                    continue
                collumnsInLookingFors = []
                for key, value in  self.colPositions.items():
                    if key in lookingFors:
                        collumnsInLookingFors.append(value)
                rows.append([row[i] for i in collumnsInLookingFors])
                lookAtLater.pop(index)

        return rows
    
    def getByIndex(self, index: int) -> any:
        return self.tableContents[index]
        
    def insert(self, colName, word, id):
        if colName not in self.trees:
            raise KeyError(f"Column {colName} not found in tables tree")
        node = self.trees[colName]
        for char in word:
            node = node[char]
        node["id"] = id

    def getIndexIfExists(self, colName, colValue):
        if colName not in self.trees:
            raise KeyError(f"Column {colName} not found in tables tree")
        node = self.trees[colName]
        for char in colValue:
            if char in node:
                node = node[char]
            else:
                return False
        if "id" in node:
            return node["id"]
        return False
    
    def getByIndexIfExists(self, colName, colValue):
        if colName not in self.trees:
            raise KeyError(f"Column {colName} not found in tables tree")
        node = self.trees[colName]
        for char in colValue:
            if char in node:
                node = node[char]
            else:
                return False
        if "id" in node:
            return self.tableContents[node["id"]]
        return False
    
    def updateFirst(self, field, to, where, equals):
        """as of now only takes a single clause for each param"""
        if field not in self.colPositions:
            raise KeyError(f"Column {field} not found in table")
        if where not in self.colPositions:
            raise KeyError(f"Column {where} not found in table")
        
        for row in self.tableContents:
            if row[self.colPositions[where]] == equals:
                if self.updating[row[self.colPositions["id"]]]:
                    continue
                self.updating[row[self.colPositions["id"]]] = True
                row[self.colPositions[field]] = to
                self.updating[row[self.colPositions["id"]]] = False
                break

    def updateAll(self, field, to, where, equals):
        """as of now only takes a single clause for each param"""
        if field not in self.colPositions:
            raise KeyError(f"Column {field} not found in table")
        if where not in self.colPositions:
            raise KeyError(f"Column {where} not found in table")
        
        for row in self.tableContents:
            if row[self.colPositions[where]] == equals:
                if self.updating[row[self.colPositions["id"]]]:
                    continue
                self.updating[row[self.colPositions["id"]]] = True
                row[self.colPositions[field]] = to
                self.updating[row[self.colPositions["id"]]] = False


dataTypes = {"int": int, "str": str, "float": float, "bool": bool, "text": str, "date": str}

def execute(query: str):
    words = query.split(" ")
    if words[:2] == ["create", "table"]:
        tableName = words[2]
        inBrackets = query.split("(")[1].split(")")[0].split(",")
        inBrackets = [word.split(" ") for word in inBrackets]
        cols = [Col(colName, dataTypes[colType]) for colName, colType in inBrackets]
        tables[tableName] = Table(*cols)

    if words[0] == ".tables":
        print(list(tables.keys()))

    if words[:2] == ["drop", "table"]:
        tableName = words[2]
        del tables[tableName]

    if words[:3] == ["Select", "*", "from"]:
        tableName = words[2]
        print(tables[tableName].getAll())



execute("create table users (username text,password text)")
execute(".tables")
execute("drop table users")
execute(".tables")


if __name__ == "__main__":


    tree = returnsDefaultDict()


    # def stringToQuery(query: str):
    #     words = query.split(" ")
    #     if words[:2] == ["create", "table"]:
    #         tableName = words[2]
    #         colNames = words[3:-1]



    users = Table(Col("username", str), Col("password", str))
    users.createRow(("username", "password"), ("shmuli", "keller"))
    users.createRow(("username", "password"), ("bunny", "keller"))
    users.createRow(("username", "password"), ("shevi", "keller"))
    users.createRow(("username", "password"), ("shevi", "dude"))
    users.createRow(("username", "password"), ("shevi", "ngix"))


    # print(f"\n"*5)
    # print(f"{users.tableContents = }")
    # print(f"{users.getAll(None, ['password'], ['keller']) = }")
    # print(f"{users.getAll(['username'], ['password'], ['keller']) = }")
    # print(f"{users.getByIndex(0) = }")
    # print(f"\n"*5)

    # print("\n"*5)
    # print(f"{users.getByIndexIfExists('username', 'shmuli') = }")
    # print(f"{users.getByIndexIfExists('username', 'bunny') = }")
    # users.updateFirst("password", "ngix", "username", "bunny")
    # print(f"{users.getAll(['username'], ['password'], ['keller']) = }")
    # print(f"{users.getFirst(None, ['password'], ['ngix']) = }")
    # print(f"{users.getFirst(['username'], ['password'], ['ngix']) = }")


        





    """create a tree for indexing"""
    """make a query function"""
    """do databases have an in built way of stopping race conditions?"""
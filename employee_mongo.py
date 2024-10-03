from pymongo import MongoClient

# Connect to MongoDB (adjust the URI if needed)
client = MongoClient("mongodb://localhost:27017/")
db = client['employee_database']

# Function to create a collection
def createCollection(p_collection_name):
    if p_collection_name not in db.list_collection_names():
        db.create_collection(p_collection_name)
        print(f"Collection '{p_collection_name}' created.")
    else:
        print(f"Collection '{p_collection_name}' already exists.")

# Function to index employee data excluding a specific column
def indexData(p_collection_name, p_exclude_column):
    collection = db[p_collection_name]
    
    # Example employee data (customize as needed)
    employees = [
        {'EmployeeID': 'E02001', 'Name': 'John Doe', 'Department': 'IT', 'Gender': 'Male', 'Age': 30},
        {'EmployeeID': 'E02002', 'Name': 'Jane Doe', 'Department': 'HR', 'Gender': 'Female', 'Age': 25},
        {'EmployeeID': 'E02003', 'Name': 'Jim Beam', 'Department': 'IT', 'Gender': 'Male', 'Age': 35}
    ]

    # Insert each employee, excluding the specified column
    for employee in employees:
        if p_exclude_column in employee:
            del employee[p_exclude_column]
        collection.insert_one(employee)

    print(f"Data indexed into '{p_collection_name}', excluding column '{p_exclude_column}'.")

# Function to search for employees by a specific column
def searchByColumn(p_collection_name, p_column_name, p_column_value):
    collection = db[p_collection_name]
    results = collection.find({p_column_name: p_column_value})
    for result in results:
        print(result)

# Function to get the total employee count in a collection
def getEmpCount(p_collection_name):
    collection = db[p_collection_name]
    count = collection.count_documents({})
    print(f"Employee count in '{p_collection_name}': {count}")
    return count

# Function to delete an employee by ID
def delEmpById(p_collection_name, p_employee_id):
    collection = db[p_collection_name]
    result = collection.delete_one({'EmployeeID': p_employee_id})
    if result.deleted_count > 0:
        print(f"Employee with ID '{p_employee_id}' deleted from '{p_collection_name}'.")
    else:
        print(f"No employee found with ID '{p_employee_id}' in '{p_collection_name}'.")

# Function to get department facet (group by department and count employees)
def getDepFacet(p_collection_name):
    collection = db[p_collection_name]
    pipeline = [
        {"$group": {"_id": "$Department", "count": {"$sum": 1}}}
    ]
    result = collection.aggregate(pipeline)
    for res in result:
        print(f"Department: {res['_id']}, Count: {res['count']}")

# Execution of the provided steps

# Collection names
v_nameCollection = 'Hash_JohnDoe'  # Replace with your name
v_phoneCollection = 'Hash_1234'    # Replace with last four digits of your phone number

# Create collections
createCollection(v_nameCollection)
createCollection(v_phoneCollection)

# Get employee count before indexing
getEmpCount(v_nameCollection)

# Index data into collections, excluding certain columns
indexData(v_nameCollection, 'Department')
indexData(v_phoneCollection, 'Gender')

# Delete an employee by ID
delEmpById(v_nameCollection, 'E02003')

# Get employee count after deletion
getEmpCount(v_nameCollection)

searchByColumn(v_nameCollection, 'Department', 'IT')
searchByColumn(v_nameCollection, 'Gender', 'Male')
searchByColumn(v_phoneCollection, 'Department', 'IT')

getDepFacet(v_nameCollection)
getDepFacet(v_phoneCollection)

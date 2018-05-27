# NaboDB
NaboDB is a micro database class for python. Using this user can create a database, tables. Here permitted operationes are Insert/Update/Edit along with sorting and searching. It is a very simple besic level database with out relation. So it is not a RDBMS.
It can handle upto 1GB Data.

# Latest version Released
v 1.0

# How to work

db1 = MicroDB(db_name='testDB', file_path='/data/')

# to Create a table
db1.addTable('contact')

# to add fields to a table
db1.addFieldsByList('contact', ['id', 'name', 'email_id', 'mob_no', 'address'])

# to add value to fields of a table
db1.addFieldValueByList('contact', ['1', 'abir', 'abirlalbiswas@gmail.com', '8884974436','Pune'])
db1.addFieldValueByList('contact', ['2', 'nabo', 'i.nabanita.neogy@gmail.com', '9876556789','Kolkata'])

# to show fields of a table
db1.showFields('contact')   

# get the fields in a array
db1.getFields('contact')

# to add a new field in a table
db1.addField('contact', 'address') 

# to update value of a field in a row by a field value
db1.updateDataByMap('contact', {'address':'Pune, India'}, {'id':'1'} )

# to update value of a field in a row by a data id
db1.updateDataByDataId('contact', {'email':'i-abirlal@email.com'}, 1)

# to search value of a field(S) by map 
* It returns the DataID list
db1.searchByKeyValMap('contact', {'isActive':'1'})  # to search 

# to search value of a field(S) like by map value
print db1.searchLikeByKeyValMap ('contact', {'mob_no':'987654321'} )

# order by some field
db1.orderDataByField('contact', 'mob_no')

# delete data by DataID
db1.deleteDataByDataId('contact', 10)

# getting the max ID for data id filed 
maxId = db1.maxDataInField('contact', 'id', 'INT')

# to get data using search function 
db1.getData('contact',  db1.searchLikeByKeyValMap ('contact', {'mob_no':'888'} ))

# to commit the changes 
db1.writeDB()

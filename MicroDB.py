import os
import sys
import re
import time, datetime
import datetime

class MicroDB:
	"""A customer of ABC Bank with a checking account. Customers have the
    following properties:

    Attributes:
        DB_NAME: A string representing the customer's name.
        FILE_PATH: .
    """
	
	def __init__(self, DB_NAME, FILE_PATH = ''):
		#DB_NAME = 'ticketsData_20171129'
		#FILE_PATH = "C:\\Users\\abirlal.biswas\\Documents\\work_sgas\\py_test\\data\\"
		self.DB_NAME = DB_NAME
		self.DB_FILE_NAME = self.DB_NAME + '.ndb'
		self.FILE_PATH = FILE_PATH
		self.DB_FILE = FILE_PATH + self.DB_FILE_NAME 
		self.DBASE = {}
		self.NOERROR = True
		if os.path.isdir(self.FILE_PATH):
			if os.path.isfile(self.DB_FILE):
				self.readDB()
		else:			
			self.NOERROR = False
			print ("NDB::ALERT: No Path [%s] Found") % self.FILE_PATH
	#def createDB():
	def addTable(self, tableName):
		if self.isTableInDB(tableName):
			print ("NDB::ALERT: Table %s is already present..!!" % tableName )
			return False
		else:
			tbl = {tableName : {'fields':[], 'data':[]}}
			self.DBASE.update(tbl)
			return True
		
	def addFieldsByList(self, tableName, fieldsList):
		if self.isTableInDB(tableName):
			if len(self.DBASE[tableName]['fields']) > 0:
				print ("NDB::ALERT: Fields are already present in Table %s. Please use addField()..!!" % tableName )
				return False
			else:
				self.DBASE[tableName].update({ 'fields' : fieldsList })
				if not 'data' in self.DBASE[tableName]:
					self.DBASE[tableName].update({ 'data' : [] })
				return True
		else:
			return False
		
	def addField(self, tableName, field):		
		if self.isFieldPrsentInTable(tableName, field):
			fieldsList = self.DBASE[tableName]['fields']
			fieldsList.append(field)
			self.DBASE[tableName].update({ 'fields' : fieldsList })
			if 'data' in self.DBASE[tableName]:
				for i in range(len(self.DBASE[tableName]['data'])):
					self.DBASE[tableName]['data'][i].update({field:''})
			else:
				self.DBASE[tableName].update({ 'data' : [] })
			return True
		else:
			print ("NDB::ALERT: Field is already present in Table %s...!!" % tableName )
			return False
			
	def addFieldValueByList(self, tableName, fieldValueList):
		if self.isTableInDB(tableName):
			dataRow = {}
			fields = self.DBASE[tableName]['fields']
			fldValPairList = self.DBASE[tableName]['data']
			if len(fields) == len(fieldValueList):
				for i in range(len(fields)):
					dataRow.update({ fields[i] : fieldValueList[i] })
				fldValPairList.append(dataRow)
				self.DBASE[tableName].update({ 'data' : fldValPairList })
				return True
			else : 
				print ("NDB::ALERT: Number of Data is not maching with the number of fields" )
				return False
		else: 
			return False
			
	
	def updateDataByMap(self, tableName, dataMap, searchMap = {}):
		if self.isTableInDB(tableName):
			fields = self.DBASE[tableName]['fields']
			fieldPresentFlag = 1
			for key, value in dataMap.items():
				if not key in fields : 
					fieldPresentFlag = 0
					break
			if fieldPresentFlag == 1: 
				if searchMap == {}:	
					for i, e in enumerate(self.DBASE[tableName]['data']):
						self.DBASE[tableName]['data'][i].update(dataMap)
				else:
					for i, e in enumerate(self.searchByKeyValMap(tableName, searchMap)):
						self.DBASE[tableName]['data'][e].update(dataMap)
				return True	
			else:
				print "NDB::ERROR: Data Map is not propper. Field:Val pair missing...!!", dataMap
				return False
		else:
			return False
			
			
	def updateDataByDataId(self, tableName, dataMap, dataId = -1):
		if self.isTableInDB(tableName):
			fields = self.DBASE[tableName]['fields']
			fieldPresentFlag = 1
			for key, value in dataMap.items():
				if not key in fields : 
					fieldPresentFlag = 0
					break
			if fieldPresentFlag == 1: 
				if dataId == -1:	
					for i, e in enumerate(self.DBASE[tableName]['data']):
						self.DBASE[tableName]['data'][i].update(dataMap)
				else:
					self.DBASE[tableName]['data'][dataId].update(dataMap)	
				return True
			else:
				print "NDB::ERROR: Data Map is not propper. Field:Val pair missing...!!? ", dataMap
				return False
		else:
			return False
			
	def deleteDataByDataId(self, tableName, dataId):
		if self.isTableInDB(tableName):
			for i in range(len(self.DBASE[tableName]['data'])):
				if i == dataId:
					self.DBASE[tableName]['data'].pop(dataId)
					return True
			print ("NDB::ERROR: Data Id is not Present in Table %s..!!! ") % tableName
			return False
		else:
			return False
		
	
	def searchByKeyValMap(self, tableName, searchMap):
		for key, value in searchMap.items():
			if not key in self.DBASE[tableName]['fields'] : 
				print "NDB::ERROR: Search Field id not found in %s" % tableName
				return []
		dataIdList = []
		for i in range(len(self.DBASE[tableName]['data'])):
			if all((k in self.DBASE[tableName]['data'][i] and self.DBASE[tableName]['data'][i][k] == v) for k,v in searchMap.iteritems()):
				dataIdList.append(i)		
		return dataIdList
		
	def searchLikeByKeyValMap(self, tableName, searchMap):
		for key, value in searchMap.items():
			if not key in self.DBASE[tableName]['fields'] : 
				print "NDB::ERROR: Search Field id not found in %s" % tableName
				return []
		dataIdList = []
		for i in range(len(self.DBASE[tableName]['data'])):
			if all((k in self.DBASE[tableName]['data'][i] and v in self.DBASE[tableName]['data'][i][k]) for k,v in searchMap.iteritems()):
				dataIdList.append(i)		
		return dataIdList
		
			
	def orderDataByField(self, tableName, orderOnKey, isInt='n', sortOrder='asc'):
		if self.isTableInDB(tableName):
			if isInt == 'y':
				decorated = [(int(dict_[orderOnKey]), dict_) for dict_ in self.DBASE[tableName]['data']]
			else:
				decorated = [(dict_[orderOnKey], dict_) for dict_ in self.DBASE[tableName]['data']]
			if sortOrder == 'desc':
				decorated.sort(reverse=True)
			else:
				decorated.sort()
			print decorated
			self.DBASE[tableName]['data'] = [dict_ for (key, dict_) in decorated]
			return True
		else:
			return False
	
	def maxDataInField(self, tableName, fieldName, isInt=''):
		returnVal = 'null'
		if self.isTableInDB(tableName):
			if isInt == 'INT':
				returnVal = 0
				decorated = [(int(dict_[fieldName]), dict_) for dict_ in self.DBASE[tableName]['data']]
			else:
				decorated = [(dict_[fieldName], dict_) for dict_ in self.DBASE[tableName]['data']]
			decorated.sort(reverse=True)
			if len(decorated) > 0:
				returnVal = decorated[0][0]
			return returnVal
		else:
			return returnVal
			
	def minDataInField(self, tableName, fieldName, isInt=''):
		returnVal = 'null'
		if self.isTableInDB(tableName):
			if isInt == 'INT':
				returnVal = 0
				decorated = [(int(dict_[fieldName]), dict_) for dict_ in self.DBASE[tableName]['data']]
			else:
				decorated = [(dict_[fieldName], dict_) for dict_ in self.DBASE[tableName]['data']]
			decorated.sort()
			if len(decorated) > 0:
				returnVal = decorated[0][0]
			return returnVal
		else:
			return returnVal
	
	def showData(self, tableName, dataIds = [], fieldNames = []):
		if self.isTableInDB(tableName):
			isFieldNamesOkFlag = 1
			if fieldNames == []:
				fieldNames = self.DBASE[tableName]['fields']
			else:
				for j in range(len(fieldNames)):
					if not fieldNames[j] in self.DBASE[tableName]['fields'] : 
						isFieldNamesOkFlag = 0
						break
			if isFieldNamesOkFlag == 1:
				if dataIds == []:
					for i in range(len(self.DBASE[tableName]['data'])):
						dataMap = self.DBASE[tableName]['data'][i]
						print "NDB::DATA_ID=>", i
						for j in range(len(fieldNames)):
							print "NDB:: %s:%s" % (fieldNames[j], dataMap[fieldNames[j]])	
				else:
					for i, e in enumerate(dataIds):
						dataMap = self.DBASE[tableName]['data'][i]
						print "NDB::DATA_ID=>", i
						for j in range(len(fieldNames)):
							print "NDB:: %s:%s" % (fieldNames[j], dataMap[fieldNames[j]])	
			else:
				print ("NDB::ERROR: Filed Names are not matching in table ", tableName )
		
	def getData(self, tableName, dataIds = [], fieldNames = []):
		
		dataMapList = []
		try:
			if self.isTableInDB(tableName):
				isFieldNamesOkFlag = True
				isSelectStar = True
				if fieldNames == []:
					fieldNames = self.DBASE[tableName]['fields']
				else:
					isSelectStar = False
					for j in range(len(fieldNames)):
						if not fieldNames[j] in self.DBASE[tableName]['fields'] : 
							isFieldNamesOkFlag = 0
							break
				if isFieldNamesOkFlag:
					if dataIds == []:
						for i in range(len(self.DBASE[tableName]['data'])):
							dataMap = self.DBASE[tableName]['data'][i]
							if isSelectStar:
								dataMapList.append(dataMap)
							else:
								dataSet = {}
								for j in range(len(fieldNames)):
									dataSet.update({fieldNames[j]: dataMap[fieldNames[j]]})
								dataMapList.append(dataSet)
					else:
						for i, e in enumerate(dataIds):
							dataMap = self.DBASE[tableName]['data'][e]
							if isSelectStar:
								dataMapList.append(dataMap)
							else:
								dataSet = {}
								for j in range(len(fieldNames)):
									dataSet.update({fieldNames[j]: dataMap[fieldNames[j]]})
								dataMapList.append(dataSet)
				else:
					print ("NDB::ALERT: Filed Names are not matching in table ", tableName )	
		except:
			print "NDB::ERROR: Fatal Error in getData()..."
		return dataMapList
			
	def showFields(self, tableName):
		print self.DBASE[tableName]['fields']
	
	def getFields(self, tableName):
		return self.DBASE[tableName]['fields']
		
	def writeDB(self):
		try:
			dbFile = open( self.DB_FILE, 'w+' )
			for tblName, tblData in self.DBASE.items():
				dbFile.write("t~%s\n" % (tblName))
				#print ("Table name: " + tblName)
				fields = 'f~'
				for i in range(len(tblData['fields'])):
					fields = fields + tblData['fields'][i] + '`'
				fields = fields[:-1]
				dbFile.write("%s\n" % (fields))
				#print (fields)
				for i, e in enumerate(tblData['data']):
					dataRow = 'd~'
					for k in range(len(tblData['fields'])):
						dVal = str(tblData['data'][i][tblData['fields'][k]])
						dVal = dVal.replace('\n','\\n')
						dVal = dVal.replace('\r','\\r')
						dataRow = dataRow + dVal + '`'
					dataRow = dataRow[:-1]
					dbFile.write("%s\n" % (dataRow))
			print "NDB::INFO: DB is saved in disk... "
			dbFile.close()
		except:
			print "NDB::ERROR: Saving in disk failed... "
		
	def readDB(self):
		fields = []
		dbFile = open( self.DB_FILE, 'r+' )
		dbFileLines=dbFile.readlines()
		
		tbl = {}
		tblName = ''
		fields = []
		fieldData = []
		fldValPairList = []
		for line in dbFileLines:
			if 't~' in line:
				tblName = re.match(r"t~(.*)", line).group(1)
				self.addTable(tblName)
			if 'f~' in line:
				fldValPairList = []
				fieldsStr = re.match(r"f~(.*)", line).group(1)
				fields = fieldsStr.split('`')
				self.addFieldsByList(tblName, fields)
				#print fields
			if 'd~' in line:
				dataRowStr = re.match(r"d~(.*)", line).group(1)
				fieldData = dataRowStr.split('`')
				fieldData = [word.replace('\\n','\n') for word in fieldData]
				fieldData = [word.replace('\\r','\r') for word in fieldData]
				self.addFieldValueByList(tblName, fieldData)
		dbFile.close()
	
	def isTableInDB(self, tableName):
		if tableName in self.DBASE:
			return True
		else:
			return False
	
	def isFieldPrsentInTable(self, tableName, field):
		if isTableInDB(tableName):
			if field in self.DBASE[tableName]['fields']:
				return True
			else:
				print ("NDB::ALERT: No table %s present ") % tableName 
				return False
		else:
			print ("NDB::ALERT: %s is already present as Table...!!" % tableName )
			return False
	
#============================================================================
	def printDbTree(self):
		self.walk_dict(self.DBASE)
		#print(self.DBASE)
		
	def walk_dict(self, d):
		for k,v in d.items():
			if k not in ('fields', 'data'):
				print ("NDB::Info Table name: " + k)
			if isinstance(v, dict):
				self.walk_dict(v)
			else:
				print "%s: %s" % (k, v) 



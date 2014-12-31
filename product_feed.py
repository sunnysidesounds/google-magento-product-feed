#! /usr/bin/env python

################################################################################
## NAME: PRODUCT FEED 
## DATE: Feb 2012
## AUTHOR: Jason R Alexander
## MAIL: sunnysidesounds@gmail.com
## INFO: This script will send product information from Magento EAV schema format to XML, 
## upload the XML to google for google's product search. 
##
## NOTE : This is just a generic reference and may require alteration to later version of Magento
#################################################################################

import sys
import os
import oursql
import string
import time
import base64
import csv
import datetime
from urllib import urlopen
import logging
import glob
import traceback
import ast

#email
import smtplib
from email import Encoders
from email.MIMEBase import MIMEBase
from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText
from email.Utils import formatdate
#zip
import zipfile
#ftp
import ftplib
#copy files
import shutil



class productFeed():
	""" This script will send both Google Analytics and GoDataFeed"""
	# --------------------------------------------------------------------------------------------------------------------------------------------------------------- #
	def __init__(self):
	    #DB
		self.dbHost1 = '<Read_DB1_HOST>' #<- Reads mysqlview5 
		self.dbHost2 = '<Read_DB1_HOST>' #<-Reads mysqlview0
		self.dbWriteProd = '<Production_DB_HOST>' # <- Inserts to mysql0
		self.dbWriteDev = '<Development_DB_HOST>' # <- Inserts to  development
				
	    #DB
		self.database = '<production_db_name>'
		self.devDatabase = '<development_db_name'
		self.dbUsername = base64.b64decode("cm9vdA==")
		self.dbPassword = base64.b64decode("cmJVY0M3c1E3Umt4")
		#FTP
		self.ftpEnabled = 1
		self.ftpGoogleHost = 'uploads.google.com'
		self.ftpGoogleUser = '<google_username>'
		self.ftpGooglePass = '<google_password>'
		self.ftpGoToHost = 'uploads.godatafeed.com'
		self.ftpGoToUser = '<goto_data_username>'
		self.ftpGoToPass = '<goto_data_password>'

		#Email
		self.fromEmail = 'no-reply-product-feed@<your_domain>.com'
		self.emailList = ['youremail@domain.com']
	    
		#MISC
		self.baseUrl = 'http://www.yourdomain.com/'
		self.imgBaseUrl = '<base_img_url>'
		self.googleTrackLink = "?gclid={SI:gclid}"
		self.logAppName = 'Product Feed'
		self.spotCheckList = [100, 500, 800, 1000, 2500, 5000, 7500, 10000, 12500, 15000, 20000, 20500, 21000]
		
		#FILE AND PATHS
		self.fileWebDirectory = "<file_path_to_store_generated_files>"
		#self.fileDirectory = "/home/jasona/python/product_feed/"
		#Get directory of the script being run
		self.fileDirectory = str(os.path.dirname(os.path.abspath(__file__))) + "/"
		self.fileOutCat = self.fileDirectory+"<catalog_file_directory>"
		self.fileOutProd = self.fileDirectory+"products.xml"
		self.fileOutExclusions = self.fileDirectory+"exclusions.csv"
		self.fileNewCat = self.fileDirectory+"new_categories.csv"
		self.fileWoCat = self.fileDirectory+"wo_categories.csv"
		self.fileTmp = self.fileDirectory+"tmp.csv"
		self.fileTmp2 = self.fileDirectory+"tmp2.csv"
		self.fileLog = self.fileDirectory+"product_feed_log.txt"
		self.fileReport = self.fileDirectory+"feed_report.csv"
		
	    
	#Database methods
	# --------------------------------------------------------------------------------------------------------------------------------------------------------------- #
	def dbConnect(self):
		"""Basic sql connect which creates a cursor to execute queries """
		#try first host
		try:
			conn = oursql.connect(host = self.dbHost1, user=self.dbUsername, passwd=self.dbPassword,db=self.database, use_unicode=False, charset=None, port=3306)
		#if no first host, try second host
		except:
			conn = oursql.connect(host = self.dbHost2, user=self.dbUsername, passwd=self.dbPassword,db=self.database, use_unicode=False, charset=None, port=3306)		
		curs = conn.cursor(oursql.DictCursor)
		curs = conn.cursor(try_plain_query=False)
		
		return curs

	# --------------------------------------------------------------------------------------------------------------------------------------------------------------- #
	def dbWriteConnectProd(self):
		"""Basic sql connect which creates a cursor to execute queries """
		#try first host
		conn = oursql.connect(host = self.dbWriteProd, user=self.dbUsername, passwd=self.dbPassword,db=self.database, use_unicode=False, charset=None, port=3306)
		curs = conn.cursor(oursql.DictCursor)
		curs = conn.cursor(try_plain_query=False)		
		return curs

	# --------------------------------------------------------------------------------------------------------------------------------------------------------------- #
	def dbWriteConnectDev(self):
		"""Basic sql connect which creates a cursor to execute queries """
		#try first host
		conn = oursql.connect(host = self.dbWriteDev, user=self.dbUsername, passwd=self.dbPassword,db=self.devDatabase, use_unicode=False, charset=None, port=3306)
		curs = conn.cursor(oursql.DictCursor)
		curs = conn.cursor(try_plain_query=False)		
		return curs
		
	# --------------------------------------------------------------------------------------------------------------------------------------------------------------- #
	def dbGetAllTable(self, table):
		"""Returns the first 100 records, for table testing """
		cursor = self.dbConnect()
		query = 'SELECT * FROM `'+table+'`LIMIT 0, 10'
		cursor.execute(query, plain_query=False)
		dataDic = cursor.fetchall()		
		return dataDic

	# --------------------------------------------------------------------------------------------------------------------------------------------------------------- #
	def dbGetCategoryDiff(self):
		"""This get the category differences between google_map table and magento table """
		dbList = []
		cursor = self.dbConnect()
		query = """SELECT entity_id, name, path FROM catalog_category_flat_store_1 WHERE level >= 3 AND is_active = 1 AND entity_id NOT IN (SELECT mcat_id FROM google_map)"""
		cursor.execute(query, plain_query=False)
		dataDic = cursor.fetchall()		
		return dataDic

	# --------------------------------------------------------------------------------------------------------------------------------------------------------------- #
	def dbProductType(self, pid):
		"""Returns the product type """
		cursor = self.dbConnect()
		query = 'SELECT value FROM catalog_category_entity_varchar WHERE attribute_id = 31 AND entity_id = '+pid+''
		cursor.execute(query, plain_query=False)
		dataDic = cursor.fetchall()		
		cursor.close()
		return dataDic[0][0]

	# --------------------------------------------------------------------------------------------------------------------------------------------------------------- #
	def dbProductTypeAlt(self, pid):
		"""Returns the product type """
		cursor = self.dbConnect()
		query = 'SELECT path FROM catalog_category_entity WHERE entity_id = '+pid+''
		cursor.execute(query, plain_query=False)
		for value in cursor:
			return str(value[0])

	# --------------------------------------------------------------------------------------------------------------------------------------------------------------- #
	def dbProductUrl(self, parentid):
		"""Returns the product url """
		cursor = self.dbConnect()
		query = """SELECT cp.value FROM catalog_product_entity_varchar cp
		INNER JOIN core_url_rewrite re ON cp.entity_id = re.product_id
		WHERE attribute_id = 82 AND concat(cp.value, '.html') = re.request_path AND cp.entity_id = ' """+parentid +"""' """				
		cursor.execute(query, plain_query=False)		
		for value in cursor:
			url = self.baseUrl + value[0] + '.html' + self.googleTrackLink
			return url

	# --------------------------------------------------------------------------------------------------------------------------------------------------------------- #
	def dbProductUrlAlt(self, entityid):
		"""Returns the product url """
		cursor = self.dbConnect()
		query = """SELECT cp.value FROM catalog_product_entity_varchar cp WHERE attribute_id = 83 AND cp.entity_id = '"""+entityid +"""' """				
		cursor.execute(query, plain_query=False)		
		for value in cursor:	
			url = self.baseUrl + value[0] + self.googleTrackLink
			return url

	# --------------------------------------------------------------------------------------------------------------------------------------------------------------- #
	def dbProductUrlAlt2(self, parentid):
		"""Returns the product url """
		cursor = self.dbConnect()
		query = """SELECT request_path FROM core_url_rewrite WHERE category_id IS NULL AND product_id = '"""+parentid +"""' """
		cursor.execute(query, plain_query=False)		
		for value in cursor:
			url = self.baseUrl + value[0] + '.html' + self.googleTrackLink
			return url

	# --------------------------------------------------------------------------------------------------------------------------------------------------------------- #
	def dbProductMissingCategories(self):
		"""This gets all products that don't have categories"""
		missing_product_categories = pf.dbAllSimples("AND cpe.category_ids IS NULL")
		return missing_product_categories

		
	# --------------------------------------------------------------------------------------------------------------------------------------------------------------- #
	def dbProductCategories(self, productid):
		"""This get the category name """
		dbList = []
		cursor = self.dbConnect()
		query = """SELECT ccev.value category FROM 
					catalog_product_entity
					LEFT JOIN catalog_category_entity_varchar ccev 
					ON FIND_IN_SET(ccev.entity_id, catalog_product_entity.category_ids) 
						AND attribute_id = 31
					WHERE catalog_product_entity.entity_id = """ + productid
		
		cursor.execute(query, plain_query=False)
		for value in cursor:
			dbList.append(value[0])
		return dbList

	# --------------------------------------------------------------------------------------------------------------------------------------------------------------- #
	def dbGetGoogleMapValue(self, catid):
		"""This get the google category """
		cursor = self.dbConnect()
		query = "SELECT gcat_id FROM google_map WHERE mcat_id = '"+ catid +"'"		
		cursor.execute(query, plain_query=False)
		for value in cursor:
			return value[0]

	# --------------------------------------------------------------------------------------------------------------------------------------------------------------- #
	def dbGetTitle(self, pid):
		"""This get the google category """
		cursor = self.dbConnect()
		query = "SELECT value FROM catalog_product_entity_varchar WHERE attribute_id = 56 AND entity_id = '"+ pid +"'"		
		cursor.execute(query, plain_query=False)
		for value in cursor:
			return value[0]
		
	# --------------------------------------------------------------------------------------------------------------------------------------------------------------- #
	def dbCheckGoogleCategoryProd(self, pid):
		"""This will check if the pid already has a google category per production """
		cursor = self.dbConnect()
		query = "SELECT value FROM catalog_product_entity_varchar WHERE attribute_id = 733 AND entity_id = '"+ pid +"'"		
		cursor.execute(query, plain_query=False)
		for value in cursor:
			return value

	# --------------------------------------------------------------------------------------------------------------------------------------------------------------- #
	def dbCheckGoogleCategoryDev(self, pid):
		"""This will check if the pid already has a google category per dev """
		cursor = self.dbWriteConnectDev() 
		query = "SELECT value FROM catalog_product_entity_varchar WHERE attribute_id = 733 AND entity_id = '"+ pid +"'"		
		cursor.execute(query, plain_query=False)
		for value in cursor:
			return value

	# --------------------------------------------------------------------------------------------------------------------------------------------------------------- #
	def dbInsertGoogleCategoryProd(self, entity_id, value):
		"""This inserts all values into google mapping per production"""
		cursor = self.dbWriteConnectProd()		
		inDev = self.dbCheckGoogleCategoryProd(entity_id)		
		#If there is a record in dev let's update instead of inserting
		if(inDev): #Do not remove 733 from this query and run it. You could do serious damage on the product attributes.
			query = "UPDATE catalog_product_entity_varchar SET value = '"+ value +"' WHERE attribute_id = 733 AND entity_id = '"+ entity_id +"'"
			kind_of_query = "updating"
		else:
			query = "INSERT INTO catalog_product_entity_varchar (entity_type_id, attribute_id, store_id, entity_id, value) VALUES ('4', '733', '0', '"+ entity_id +"', '"+ value +"')"
			kind_of_query = "inserting"				
		try:
			cursor.execute(query, plain_query=False)
			self.log('Success in '+ kind_of_query +' pid into Prod: ' + str(entity_id) + ' with value: ' + str(value))
		except:
			self.log('Could not '+ kind_of_query +' pid into Prod:' + str(entity_id) + ' with value:' + str(value))

	# --------------------------------------------------------------------------------------------------------------------------------------------------------------- #
	def dbInsertGoogleCategoryDev(self, entity_id, value):
		"""This inserts all values into google mapping per dev"""
		cursor = self.dbWriteConnectDev()		
		inDev = self.dbCheckGoogleCategoryDev(entity_id)		
		#If there is a record in dev let's update instead of inserting
		if(inDev): #Do not remove 733 from this query and run it. You could do serious damage on the product attributes.
			query = "UPDATE catalog_product_entity_varchar SET value = '"+ value +"' WHERE attribute_id = 733 AND entity_id = '"+ entity_id +"'" 
			kind_of_query = "updating"
		else:
			query = "INSERT INTO catalog_product_entity_varchar (entity_type_id, attribute_id, store_id, entity_id, value) VALUES ('4', '733', '0', '"+ entity_id +"', '"+ value +"')"
			kind_of_query = "inserting"				
		try:
			cursor.execute(query, plain_query=False)
			self.log('Success in '+ kind_of_query +' pid into Dev: ' + str(entity_id) + ' with value: ' + str(value))
		except:
			self.log('Could not '+ kind_of_query +' pid into Dev:' + str(entity_id) + ' with value:' + str(value))

		
	# --------------------------------------------------------------------------------------------------------------------------------------------------------------- #
	def dbRatingsCount(self, parentid):
		"""Returns the product rating """
		if(str(parentid) != "None"):			
			cursor = self.dbConnect()
			query = """SELECT count(rate_id) as rateCount FROM catalog_product_rate 
						WHERE product_id = """ + parentid			
			cursor.execute(query, plain_query=False)		
			for value in cursor:
				if value[0] != 0:
					out = str(value[0])
				else:
					out = ''
		else:
			out = ''		
		return out

	# --------------------------------------------------------------------------------------------------------------------------------------------------------------- #
	def dbRatingsAverage(self, parentid):
		"""Returns the product rating """
		if(str(parentid) != "None"):			
			cursor = self.dbConnect()
			query = """SELECT avg(score) as rateAvg FROM catalog_product_rate 
						WHERE product_id = """ + parentid			
			cursor.execute(query, plain_query=False)		
			for value in cursor:
				if value[0] is not None :
				#TODO: round values
					out = str(value[0])
				else:
					out = ''
		else:
			out = ''		

		return out
			
	# --------------------------------------------------------------------------------------------------------------------------------------------------------------- #
	def dbAllSimples(self, add_and = "", limit = ""):
		"""This gets the entity ID from the simple sku"""		
		cursor = self.dbConnect()
		query = """SELECT cpe.entity_id id, cpe.sku, cpe.type_id, cpe.category_ids, cpev_title.value title, cpet_description.value description, cped_price.value price,
				css.qty, cce.path product_type, eav_vendor.value vendor, atbv.description brand, atbc.description color , atbs.description size, 
				cped_weight.value weight, cpev_upc.value upc, cpev_mpn.value mpn, cped_sale_price.value sale_price, YEAR(cped_year.value) atb_year,
				cpsl.parent_id, atbc.id color_id, parent.sku parent_sku, cpei_status.value status, cpev_google.value google
			FROM catalog_product_entity cpe 
			LEFT JOIN cataloginventory_stock_status css  ON cpe.entity_id = css.product_id 
			LEFT JOIN catalog_product_super_link cpsl  ON cpe.entity_id = cpsl.product_id 
			LEFT JOIN catalog_product_entity parent ON parent.entity_id = cpsl.parent_id 
			LEFT JOIN catalog_product_entity_varchar cpev_title ON cpe.entity_id = cpev_title.entity_id AND  cpev_title.attribute_id = 56 
			LEFT JOIN catalog_product_entity_text cpet_description ON cpe.entity_id = cpet_description.entity_id AND  cpet_description.attribute_id = 57 
			LEFT JOIN catalog_product_entity_decimal cped_price ON cpe.entity_id = cped_price.entity_id AND  cped_price.attribute_id = 60 
			LEFT JOIN catalog_category_entity cce ON cce.entity_id IN (cpe.category_ids) 
			LEFT JOIN catalog_product_entity_int cpei_vendor ON cpe.entity_id = cpei_vendor.entity_id AND  cpei_vendor.attribute_id = 66 
			LEFT JOIN eav_attribute_option_value eav_vendor on (cpei_vendor .value = eav_vendor.option_id and eav_vendor .store_id = 1) 
			LEFT JOIN atb_vendors atbv ON atbv.option_id = cpei_vendor.value 
			LEFT JOIN catalog_product_entity_int cpei_color ON cpe.entity_id = cpei_color.entity_id AND  cpei_color.attribute_id = 76 
			LEFT JOIN atb_colors atbc ON atbc.option_id = cpei_color.value 
			LEFT JOIN catalog_product_entity_int cpei_size ON cpe.entity_id = cpei_size.entity_id AND  cpei_size.attribute_id = 491
			LEFT JOIN catalog_product_entity_int cpei_status ON cpe.entity_id = cpei_status.entity_id AND  cpei_status.attribute_id = 80
			LEFT JOIN atb_sizes atbs ON atbs.option_id = cpei_size.value 
			LEFT JOIN catalog_product_entity_decimal cped_weight ON cpe.entity_id = cped_weight.entity_id  AND cped_weight.attribute_id = 65 
			LEFT JOIN catalog_product_entity_varchar cpev_upc ON cpe.entity_id = cpev_upc.entity_id  AND cpev_upc.attribute_id = 509 
			LEFT JOIN catalog_product_entity_varchar cpev_mpn ON cpe.entity_id = cpev_mpn.entity_id  AND cpev_mpn.attribute_id = 501
			LEFT JOIN catalog_product_entity_varchar cpev_google ON cpe.entity_id = cpev_google.entity_id  AND cpev_google.attribute_id = 733 
			LEFT JOIN catalog_product_entity_decimal cped_sale_price ON cpe.entity_id = cped_sale_price.entity_id  AND cped_sale_price.attribute_id = 61  
			LEFT JOIN catalog_product_entity_datetime cped_year ON cpe.entity_id = cped_year.entity_id  AND cped_year.attribute_id = 553  
			WHERE cpe.type_id = 'simple' AND css.stock_status = 1 AND css.qty > 0 AND cpei_status.value = 1 """+ add_and +"""
			GROUP BY cpe.entity_id """+ limit +"""
		"""		
		cursor.execute(query, plain_query=False)
		data = cursor.fetchall()
		#close this connection after query
		cursor.close()	
		return data

	#XML methods
	# --------------------------------------------------------------------------------------------------------------------------------------------------------------- #
	def xmlGoToFirst(self):
		"""GoToData XML First """		
		out = "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n"
		out += "<GoDataFeed>\n"
		out += "  <Fields>\n"
		out += "    <Field name=\"UniqueID\"/>\n"
		out += "    <Field name=\"Name\"/>\n"
		out += "    <Field name=\"Description\"/>\n"
		out += "    <Field name=\"Price\"/>\n"
		out += "    <Field name=\"Sale_Price\"/>\n"
		out += "    <Field name=\"MerchantCategory\"/>\n"
		out += "    <Field name=\"URL\"/>\n"
		out += "    <Field name=\"ImageURL\"/>\n"
		out += "    <Field name=\"Manufacturer\"/>\n"
		out += "    <Field name=\"ManufacturerPartNumber\"/>\n"
		out += "    <Field name=\"Quantity\"/>\n"
		out += "    <Field name=\"Condition\"/>\n"
		out += "    <Field name=\"UPC\"/>\n"
		out += "    <Field name=\"Size\"/>\n"
		out += "    <Field name=\"Color\"/>\n"
		out += "    <Field name=\"Weight\"/>\n"
		out += "    <Field name=\"SKU\"/>\n"
		out += "    <Field name=\"Product_Review_Count\"/>\n"
		out += "    <Field name=\"Product_Review_Average\"/>\n"
		out += "    <Field name=\"Year\"/>\n"
		out += "  </Fields>\n"
		out += "  <Products>"

		return out

	# --------------------------------------------------------------------------------------------------------------------------------------------------------------- #
	def xmlGoToMiddle(self, pid, title, description, price, sale_price, product_type, product_url, product_img_url, manufacturer, mpn, qty, upc, size, color, weight, sku, ratings_count, ratings_average, year):
		"""GoToData XML Middle (Products) Template """
		out = "     <Product>\n"
		out += "       <UniqueID><![CDATA[" + pid + "]]></UniqueID>\n"
		out += "       <Name><![CDATA[" + title + "]]></Name>\n"
		out += "       <Description><![CDATA[" + description + "]]></Description>\n"
		out += "       <Price><![CDATA[" + price + "]]></Price>\n"
		out += "       <Sale_Price>" + sale_price + "</Sale_Price>\n"
		out += "       <MerchantCategory><![CDATA[" +product_type + "]]></MerchantCategory>\n"
		out += "       <URL><![CDATA[" + product_url +"]]></URL>\n"
		out += "       <ImageURL><![CDATA[" + product_img_url  + "]]></ImageURL>\n"
		out += "       <Manufacturer><![CDATA[" + manufacturer + "]]></Manufacturer>\n"
		out += "       <ManufacturerPartNumber><![CDATA[" + mpn + "]]></ManufacturerPartNumber>\n"
		out += "       <Quantity><![CDATA[" + qty + "]]></Quantity>\n"
		out += "       <Condition><![CDATA[new]]></Condition>\n"
		out += "       <UPC><![CDATA[" + upc + "]]></UPC>\n"
		out += "       <Size><![CDATA[" + size + "]]></Size>\n"
		out += "       <Color><![CDATA[" + color + "]]></Color>\n"
		out += "       <Weight><![CDATA[" + weight + "]]></Weight>\n"
		out += "       <SKU><![CDATA[" + sku + "]]></SKU>\n"
		out += "       <Product_Review_Count><![CDATA[" + ratings_count + "]]></Product_Review_Count>\n"
		out += "       <Product_Review_Average><![CDATA[" + ratings_average + "]]></Product_Review_Average>\n"
		out += "       <Year><![CDATA[" + year + "]]></Year>\n"
		out += "     </Product>"
		
		return out

	# --------------------------------------------------------------------------------------------------------------------------------------------------------------- #
	def xmlGoToLast(self, count):
		"""GoToData XML Last """
		out = "   </Products>\n"
		out += "   <Paging>\n"
		out += "     <Start>1</Start>\n"
		out += "     <Count>" + str(count) + "</Count>\n"
		out += "     <Total>" + str(count) + "</Total>\n"
		out += "   </Paging>\n"
		out += "</GoDataFeed>\n"
		
		return out

	# --------------------------------------------------------------------------------------------------------------------------------------------------------------- #
	def xmlGoogleFirst(self):
		"""Google XML Start """
		out = "<?xml version='1.0'?>\n"
		out += " <rss version='2.0' xmlns:g='http://base.google.com/ns/1.0' xmlns:c='http://base.google.com/cns/1.0'>\n";
		out += "    <channel>"
		
		return out			
	
	# --------------------------------------------------------------------------------------------------------------------------------------------------------------- #
	def xmlGoogleMiddle(self, pid, title, description, price, sale_price, product_type, product_url, product_img_url, manufacturer, mpn, qty, upc, size, color, weight, sku, ratings_count, ratings_average, year, parent_sku, product_multi_img_url, gender, google_category):
		"""Google XML Middle (Products) Template """
		out = "        <item>\n"
		out += "        	  <title><![CDATA[" + title + "]]></title>\n"
		out += "        	  <description><![CDATA[" + description + "]]></description>\n"
		out += "        	  <link><![CDATA[" + product_url  + "]]></link>\n"
		out += "        	  <g:id>"+ pid +"</g:id>\n"
		out += "        	  <g:image_link><![CDATA[" + product_img_url + "]]></g:image_link>\n"
		
		#Convert a string list to an actual python list from csv
		product_multi_img_url = ast.literal_eval(product_multi_img_url)
		
		for additional_image_link in product_multi_img_url:
			if(additional_image_link != product_img_url):
				out += "        	  <g:additional_image_link><![CDATA[" + additional_image_link + "]]></g:additional_image_link>\n"
		
		#if sale price is present lets show this instead of sale price		
		if sale_price is not None and sale_price.strip() !="":
			out += "                  <g:price>" + sale_price + "</g:price>\n"
		else:
			out += "                  <g:price>" + price + "</g:price>\n"				
		
		out += "        	  <g:product_type><![CDATA[" + product_type + "]]></g:product_type>\n"
		out += "        	  <g:brand><![CDATA[" + manufacturer + "]]></g:brand>\n"
		out += "        	  <g:condition>new</g:condition>\n"
		out += "        	  <g:quantity>" + qty + "</g:quantity>\n"
		out += "        	  <g:color><![CDATA[" + color  + "]]></g:color>\n"
		if(size):
			out += "        	  <g:size><![CDATA["+ size + "]]></g:size>\n"
		if(weight):
			out += "        	  <g:shipping_weight>" + weight + " lbs</g:shipping_weight>\n"
		if(upc):
			out += "        	  <g:gtin><![CDATA["+ upc +"]]></g:gtin>\n"
		if(mpn):			
			out += "        	  <g:mpn><![CDATA[" + mpn + "]]></g:mpn>\n"
		if(ratings_count != ''):
			out += "        	  <g:product_review_count>" + ratings_count + "</g:product_review_count>\n"
		if(ratings_average != ''):
			out += "        	  <g:product_review_average>" + ratings_average + "</g:product_review_average>\n"
		out += "        	  <g:year>"+ year + "</g:year>\n"
		out += "        	  <g:online_only>n</g:online_only>\n"
		
		#If "boys" is in the title string, we are assuming this is a kid product. (Should probably come up with a better way to set this.)
		if(title.find('Boys') == -1 and title.find('boys') == -1):
			out += "        	  <g:age_group>adult</g:age_group>\n"
		else:
			out += "        	  <g:age_group>kids</g:age_group>\n"
		
		out += "        	  <g:gender>" + gender + "</g:gender>\n" 
		out += "        	  <g:availability>in stock</g:availability>\n"
		
		out += "	          <g:google_product_category><![CDATA[" + google_category + "]]></g:google_product_category>\n"
		out += "	          <g:item_group_id>" + parent_sku + "</g:item_group_id>\n"		
		out += "        </item>"

		return out			
	
	# --------------------------------------------------------------------------------------------------------------------------------------------------------------- #
	def xmlGoogleLast(self):
		"""Google XML Last """
		out = "  </channel>\n"
		out += "</rss>\n"
		
		return out			
	
	# --------------------------------------------------------------------------------------------------------------------------------------------------------------- #
	def xmlProducts(self, productlist, feed):
		"""Product XMl Element """
		count = 0
		
		#This runs second using the data from the gotodata build
		if(feed == "google"):
			for simpleItem in productlist:
				out = "<!-- Item " + str(count) + " -->\n"
				out += self.xmlGoogleMiddle(str(simpleItem[0]), str(simpleItem[1]), str(simpleItem[2]), str(simpleItem[3]), str(simpleItem[4]), str(simpleItem[5]), str(simpleItem[6]), str(simpleItem[7]), str(simpleItem[8]), str(simpleItem[9]), str(simpleItem[10]), str(simpleItem[11]), str(simpleItem[12]), str(simpleItem[13]), str(simpleItem[14]), str(simpleItem[15]), str(simpleItem[16]), str(simpleItem[17]), str(simpleItem[18]), str(simpleItem[19]), str(simpleItem[20]), str(simpleItem[21]), str(simpleItem[22]))
				write = pf.out(out, pf.fileOutCat)
				count = count + 1
				#Debugger log
				if count in self.spotCheckList:
					self.log("Total products built for " + str(feed) + " is " + str(count))			
					
		#This runs first querying the DB
		elif(feed == "gotodata"):
		
			productList = []			
			for simpleItem in productlist:
				
				#remove Toms Shoes by brand
				if(simpleItem[10] == 'TOMS SHOES'):
					continue
								
				#Feed values			
				pid = str(simpleItem[0])
				title= str(simpleItem[4])
				description= str(simpleItem[5]) #TODO: We need to filter this for special characters
				price = self.utlRemoveEnd(str(simpleItem[6]), '00')				
				sale_price = self.checkSalesPrice(simpleItem[16], pid)
				category_ids = str(simpleItem[3])
				product_type = self.checkProductType(simpleItem[8], pid, category_ids)
				product_url = self.checkProductUrl(str(simpleItem[18]), str(pid))
				sku = str(simpleItem[1])
				color_id = str(simpleItem[19])
				product_img_url = self.productImageUrl(color_id, sku)				
				product_multi_img_url = self.productImageMultiUrl(color_id, sku)				
				brand = str(simpleItem[10])			
				vendor = str(simpleItem[9])
				manufacturer = self.checkVendorBrand(vendor, brand, pid)							
				mpn = self.checkEmpty(str(simpleItem[15]), pid) 
				qty = self.checkQty(str(simpleItem[7]), pid)
				upc = self.removeSciNote(self.checkEmpty(str(simpleItem[14]), pid))	
				#upc = self.checkEmpty(str(simpleItem[14]), pid)			
				
				
				
				size = self.checkEmpty(str(simpleItem[12]), pid) 
				color = self.checkEmpty(str(simpleItem[11]), pid)			
				weight = self.checkEmpty(str(simpleItem[13]), pid)
				parent_id = str(simpleItem[18])									
				ratings_count = self.checkRatingsCount(parent_id, pid)
				ratings_average = self.checkRatingsAverage(parent_id ,pid) #TODO <-- Round value							
				year = self.checkEmpty(str(simpleItem[17]), pid)
				type_id = str(simpleItem[2]) #simple/configurable														
				gender = self.productGenderByCat(pid) #For Google			
				parent_sku = str(simpleItem[20])			
				status = str(simpleItem[21])				
				google_category = str(simpleItem[22])
				
				
				
							
				#INSERT ON FLY FUNCTIONALITY
				#If google category is empty lets insert the value and retrive for the feed
				if(google_category == ""):					
					insert_on_fly = self.googleCategoryInsert(category_ids, pid)
					#Let's wait so the DB slaves can replicate.
					time.sleep(3) 
					#Get the new category
					new_google_category = self.dbCheckGoogleCategoryProd(pid)
					
					if(new_google_category is not None):
						google_category = new_google_category[0]
					else:
						google_category = ''
																										
				#These are value you just want to extract on a condition. Mostly if it's empty. These values will still pass into the feed.
				alternateValueDic = {}
				for keyz, valuez in alternateValueDic.iteritems():				
					if(alternateValueDic[keyz] == ""):
						self.csvWrite(["no " + keyz, pid, sku, title, brand, product_type, product_url, product_img_url, price, sale_price, qty, color, size, year], self.fileOutExclusions)
						self.log("This product is being tracked: [no " + key + "] [pid:" + pid + "] [title:" + title + "]")				
													
				#The required values list holds all values that need to be in the feed.
				requiredValueDic = {'pid' : pid, 'title' : title, 'description' : description, 'price' : price, 'product_type' : product_type, 'product_img_url' : product_img_url, 'manufacturer' : manufacturer, 'google_category' : google_category, 'product_url' : product_url}			
				passList = []
				for key, value in requiredValueDic.iteritems():				
					if(requiredValueDic[key] == ""):
						self.csvWrite(["no " + key, pid, sku, title, brand, product_type, product_url, product_img_url, price, sale_price, qty, color, size, year], self.fileOutExclusions)	
						self.log("This product has been excluded: [no " + key + "] [pid:" + pid + "] [title:" + title + "]")
					else:
						passList.append('passed')	
											
				#if the number of values in the passlist equals the number of values from requireValueDic (This means no exclusion errors)			
				if (len(passList) == len(requiredValueDic.keys())):													
					#tmp2.csv
					self.csvWrite([pid, title, description, price, sale_price, product_type, product_url, product_img_url, manufacturer, mpn, qty, upc, size, color, weight, sku, ratings_count, ratings_average, year, parent_sku, product_multi_img_url, gender, google_category], self.fileTmp2)
					#feed_report.csv
					self.csvWrite([manufacturer, color, description, product_img_url, product_url, price, sale_price, product_type, google_category, qty, size, gender], self.fileReport)
					
					#Write GoToData Products
					out = self.xmlGoToMiddle(pid, title, description, price, sale_price, product_type, product_url, product_img_url, manufacturer, mpn, qty, upc, size, color, weight, sku, ratings_count, ratings_average, year)				
					write = pf.out(out, pf.fileOutProd)									
					count = count + 1
					
					if count in self.spotCheckList:
						self.log("Total products built for " + str(feed) + " is " + str(count))
				
						
		return count
			
	#File/Csv methods
	# --------------------------------------------------------------------------------------------------------------------------------------------------------------- #
	def out(self, message, filepath):
	    """Write print output """
	    print >> fileWriter(sys.stdout, filepath), message

	# --------------------------------------------------------------------------------------------------------------------------------------------------------------- #
	def fileWrite(self, writelist, writefile):
		"""This writes the xml file """		
		for writeblock in writelist:
			self.out(writeblock, writefile)
	
	# --------------------------------------------------------------------------------------------------------------------------------------------------------------- #
	def log(self, message):
		"""This is used to write log files """
		logging.basicConfig(
			filename=self.fileLog,
			format='%(asctime)-6s: %(name)s - %(levelname)s - %(message)s')
		logger = logging.getLogger(self.logAppName)
		logger.setLevel(logging.INFO)
		logger.info(message)
		
	# --------------------------------------------------------------------------------------------------------------------------------------------------------------- #
	def csvWrite(self, row, writefile):
		"""This writes to csv file """
		errorWriter = csv.writer(open(writefile, 'a'), quoting=csv.QUOTE_ALL)				
		errorWriter.writerow(row)

	
	#Utility methods		
	# --------------------------------------------------------------------------------------------------------------------------------------------------------------- #
	def utlRemoveEnd(self, thestring, ending):
		"""Remove ending characters from a string """
  		if thestring.endswith(ending):
  			return thestring[:-len(ending)]
  		return thestring
  		
  	 # --------------------------------------------------------------------------------------------------------------------------------------------------------------- #
  	def utlFmtColorId(self, colorid):
  		"""This formats the color id for images urls """
		if(len(colorid) < 4):
			currentLength = len(colorid)
			if(currentLength == 3):
				fColor = "0" + colorid
			elif(currentLength == 2):
				fColor = "00" + colorid
			elif(currentLength == 1):
				fColor = "000" + colorid				
		else:
			fColor = colorid
			
		return fColor

	# --------------------------------------------------------------------------------------------------------------------------------------------------------------- #
	def utlAllSame(self, items):
		"""This makes sure everything in a list is the same """
		return all(x == items[0] for x in items)
	
	# --------------------------------------------------------------------------------------------------------------------------------------------------------------- #
	def utlFileExists(self, filepath):
		"""Checks to see if a file exists """
		fileExists = os.path.exists(filepath)
		if(fileExists == True):
			os.remove(filepath)
			file_create = open(filepath, 'w')
			file_create.write('')
			file_create.close()			
			reponse = '  -->  Deleting old file and recreating'
		else:
			#Create a blank file.
			file_create = open(filepath, 'w')
			file_create.write('')
			file_create.close()
			reponse = '  --> Creating new file'
		
		message = "FILE: " + filepath + reponse
		self.log(message)
		print message

	# --------------------------------------------------------------------------------------------------------------------------------------------------------------- #
	def utlSpecialChar(self, string):
		"""This removes special character we define in a string """
		string = string.replace(">", "&gt;")
		#string = string.replace("&amp;" , "")
		string = string.replace("&" , "&amp;")		
		return string
	
		
	#Validation/Checking methods
	# --------------------------------------------------------------------------------------------------------------------------------------------------------------- #
	def checkEmpty(self, value, productid):
		"""This for an empty value """
		if value is not None and value.strip() !="":			
			out = value
		else:	
			out = ""
  		return out
  		
	# --------------------------------------------------------------------------------------------------------------------------------------------------------------- #
	def checkColor(self, value, productid):
		"""This checks for a color """
		if value is not None and value.strip() !="":				
			out = value
		else:	
			out = ""
  		return out
  		
	# --------------------------------------------------------------------------------------------------------------------------------------------------------------- #
	def checkQty(self, value, productid):
		"""This checks for a quantity """
		if value is not None and value.strip() !="":				
			out = self.utlRemoveEnd(str(value), '.0000')
		else:	
			out = ""
  		return out

	# --------------------------------------------------------------------------------------------------------------------------------------------------------------- #
	def checkSalesPrice(self, value, productid):
		"""This checks for a sales price """
		if value is not None and value.strip() !="":				
			out = self.utlRemoveEnd(str(value), '00')
		else:	
			out = ""
  		return out

	# --------------------------------------------------------------------------------------------------------------------------------------------------------------- #
	def checkProductType(self, value, productid, categories):
		"""This checks to see if a product type is given"""
		
		product_type_count = 0
		category_ids_count = 0
		
		if value is not None and value.strip() !="":	
			out = self.productType(value)
		else:			
			category_id_list = categories.split(',')						
			for catid in category_id_list:			
				if catid is not None and catid.strip() !="":
					path = self.dbProductTypeAlt(catid)
				
					if path is not None and path.strip() !="":
						out = self.productType(str(path))
						break				
					else:
						product_type_count = product_type_count + 1
						out = ""
				else:				
					category_ids_count = category_ids_count + 1
					out = ""

		return out

	# --------------------------------------------------------------------------------------------------------------------------------------------------------------- #
	def checkProductUrl(self, parentid, productid):
		"""This checks to see if a product url is given, we check three different locations, if nothing we build from title """				
		if(parentid):
			url1 = self.dbProductUrlAlt2(parentid)			
			if(url1):
				out = url1
			else:
				url2 = self.dbProductUrlAlt(productid)
				if(url2):
					out = url2
				else:
					url3 = self.dbProductUrl(parentid)
					if(url3):
						out = url3
					else:
						out = ""
		else:
			url1 = self.dbProductUrlAlt(productid)				
			if(url1):
				out = url1
			else:
				#Very last resort to find the product url we build the url from the title
				out = self.baseUrl + self.convertTitleToUrl(productid) + self.googleTrackLink
							
		return out
		
	# --------------------------------------------------------------------------------------------------------------------------------------------------------------- #
	def checkRatingsCount(self, value, productid):
		"""This checks to see if ratings count is given """		
		if value is not None and value.strip() !="":
			out = self.dbRatingsCount(value)
		else:
			out = ""
		return out

	# --------------------------------------------------------------------------------------------------------------------------------------------------------------- #
	def checkRatingsAverage(self, value, productid):
		"""This checks to see if rating average is given """		
		if value is not None and value.strip() !="":
			out = self.dbRatingsAverage(value)
		else:
			out = ""		
		return out

	# --------------------------------------------------------------------------------------------------------------------------------------------------------------- #
	def checkVendorBrand(self, vendor, brand, productid):
		"""This checks to see if a product as a brand, if not get vendor name """
		if brand is not None and brand.strip() !="":
			out = brand
		else:
			if(str(vendor) != "None"):
				out = vendor
			else:
				out = ''
		return out
  		
	#Misc methods	
	# --------------------------------------------------------------------------------------------------------------------------------------------------------------- #
	def convertTitleToUrl(self, pid):
		"""This converts a title to a url """
		title = self.dbGetTitle(pid)
		lowertitle = title.lower()
		lowertitle = lowertitle.replace(" ", "-")
		lowertitle = lowertitle.replace('"', "")
		lowertitle = lowertitle.replace("'", "")
		lowertitle = lowertitle.replace("&", "and")
		lowertitle = lowertitle + ".html"
		return lowertitle
			
	# --------------------------------------------------------------------------------------------------------------------------------------------------------------- #
	def productType(self, productlist):
		"""This swaps the product_type id with their names """
		finalList = []
		plist = productlist.split('/')
		for lid in plist:
			if(self.dbProductType(lid) != 'Default Category' and self.dbProductType(lid) != 'Root Catalog'):
				finalList.append(self.dbProductType(lid))
	
		prodlist = "&gt;".join(finalList)		
		return prodlist
		
	# --------------------------------------------------------------------------------------------------------------------------------------------------------------- #
	def googleCategoryInsert(self, category_ids, pid):
		"""This sets up and inserts a google category on the fly if one isn't present, require for it to be in the google_map table """
		google_category_list = category_ids.split(",")		

		for gcat in google_category_list:
			
			google_map_value = self.dbGetGoogleMapValue(gcat)

			if google_map_value is not None:
				#Insert both in dev and production, to resolve issues with nightly product syncs
				self.dbInsertGoogleCategoryProd(pid, google_map_value)
				self.dbInsertGoogleCategoryDev(pid, google_map_value)
				out = 'Inserting value ' + str(google_map_value) + ' into catalog_product_entity_varchar for value ' + str(pid)														
			else:
				out = ''

			#To do fix this shit
			return out
						
	# --------------------------------------------------------------------------------------------------------------------------------------------------------------- #
	def productGenderByCat(self, productid):
		"""This get gender by the assoicated category """		
		nameList = self.dbProductCategories(productid)
		gender = "unisex"
					
		for name in nameList:
			if name is not None:
				if(name.find('Guys') != -1):
					gender = "male"
				elif(name.find('guys') != -1):
					gender = "male"
				elif(name.find('Girl') != -1):
					gender = "female"
				elif(name.find('girl') != -1):
					gender = "female"
				elif(name.find('Girls') != -1):
					gender = "female"
				elif(name.find('girls') != -1):
					gender = "female"
				else:
					gender = "unisex"
					
			return gender			
		
	# --------------------------------------------------------------------------------------------------------------------------------------------------------------- #
	def productCategoryDiff(self, diff_list):
		"""This formats the catefory diff list exports google_map csv  """
		self.csvWrite(["mcat_id","mcat_name","gcat_id"], self.fileNewCat)
		count = 1
		for diff in diff_list:			
			listRow = [diff[0], self.productType(diff[2]), ""]
			self.csvWrite(listRow, self.fileNewCat)
			count = count +1
		if(count != 1):		
			self.log('[completed] ' + str(count) + " new categories that need to be added to google_map")		
		return count


	# --------------------------------------------------------------------------------------------------------------------------------------------------------------- #
	def productImageUrl(self, colorid, sku):
		"""This formats and gets the main image for the product"""
		if(str(colorid) != "None" and str(sku) != "None"):
			color_sku = sku[0:6]
			color_id = self.utlFmtColorId(str(colorid))
			img_url = self.imgBaseUrl + str(color_sku) + "-" + color_id + "-front" + self.googleTrackLink
		else:
			img_url = ""
		
		return img_url

	# --------------------------------------------------------------------------------------------------------------------------------------------------------------- #
	def productImageMultiUrl(self, colorid, sku):
		"""This formats and gets all associated products """
		imageSet = []
		finalSet = []
		
		if(str(colorid) != "None" and str(sku) != "None"):
			color_sku = sku[0:6]
			color_id = self.utlFmtColorId(str(colorid))
			imageset_url = self.imgBaseUrl + str(color_sku) + "-" + color_id + "?req=imageset"
			#get image set
			set = urlopen(imageset_url).read()
			#split into a list
			imageList = set.split('/')
			
			for imgList in imageList:
				#filter out unneccessary strings
				img = self.utlRemoveEnd(str(img), "\r\n")
			
			#build alt urls
			for imgSet in imageSet:
				set = self.imgBaseUrl + imgSet + self.googleTrackLink
				finalSet.append(set)								
		else:
			finalSet = ""
			
		return finalSet

	# --------------------------------------------------------------------------------------------------------------------------------------------------------------- #
	def sendEmail(self, to_email, from_email, subject, text, file_attachment):
	    
	    HOST = "localhost"    
	    TO = to_email
	    FROM = from_email
	    ATTACH = file_attachment
	 
	    msg = MIMEMultipart()
	    msg["From"] = FROM
	    msg["To"] = TO
	    msg["Subject"] = subject
	    msg['Date']    = formatdate(localtime=True)
	    
	    msg.attach( MIMEText(text) )	 
	    # attach a file
	    part = MIMEBase('application', "octet-stream")
	    part.set_payload( open(ATTACH,"rb").read() )
	    Encoders.encode_base64(part)
	    part.add_header('Content-Disposition', 'attachment; filename="%s"' % os.path.basename(ATTACH))
	    msg.attach(part)	 
	    server = smtplib.SMTP(HOST)
	    # server.login(username, password)  # optional
	 
	    try:
	        failed = server.sendmail(FROM, TO, msg.as_string())
	        server.close()
	    except Exception, e:
	        errorMsg = "Unable to send email. Error: %s" % str(e)

	# --------------------------------------------------------------------------------------------------------------------------------------------------------------- #
	def fileLength(self, fname):
	    with open(fname) as f:
	        for i, l in enumerate(f):
	            pass
	    return i + 1
	
	# --------------------------------------------------------------------------------------------------------------------------------------------------------------- #
	def removeSciNote(self, value):
		"""The just remove all E+11 scientific notation """
		if(value.find('E+11') != -1):
			out = ""
		else: 
			out = value
		return out 
			
	# --------------------------------------------------------------------------------------------------------------------------------------------------------------- #
	def getFilesMatchingPattern(self, directory, nonWildCardPattern):
		fileList=os.listdir(directory)
		return [f for f in fileList if f.find(nonWildCardPattern) > -1]

	# --------------------------------------------------------------------------------------------------------------------------------------------------------------- #
	def ftpUpload(self, host, username, password, fullname):
		"""This uploads files to specific servers """
		ftp = ftplib.FTP()
		ftp.connect(host, 21)
		ftp.set_pasv(1)
		self.log("FTP connect to: " + host)
		self.log(ftp.getwelcome())
		print ftp.getwelcome()
		try:
			try:
				ftp.login(username, password)
				self.log("Uploading to: " + ftp.pwd())
				print "Uploading to: " + ftp.pwd()
	
				self.log("Uploading this file: " + fullname)
				print "Uploading this file: " + fullname
				name = os.path.split(fullname)[1]
				f1 = open(fullname, "rb")
				ftp.storbinary('STOR ' + name, f1)
				f1.close()
	        	
				self.log(ftp.retrlines('LIST'))
				print ftp.retrlines('LIST')
		
			finally:
				print "FTP closing connection to: " + host
				self.log("FTP closing connection to: " + host)
				ftp.quit()
		except:
			traceback.print_exc()	
	
	
	# --------------------------------------------------------------------------------------------------------------------------------------------------------------- #
	def convertBytes(self, bytes):
	    """Convert bytes to larger format """
	    bytes = float(bytes)
	    if bytes >= 1099511627776:
	        terabytes = bytes / 1099511627776
	        size = '%.2f tb' % terabytes
	    elif bytes >= 1073741824:
	        gigabytes = bytes / 1073741824
	        size = '%.2f gb' % gigabytes
	    elif bytes >= 1048576:
	        megabytes = bytes / 1048576
	        size = '%.2f mb' % megabytes
	    elif bytes >= 1024:
	        kilobytes = bytes / 1024
	        size = '%.2f kb' % kilobytes
	    else:
	        size = '%.2f b' % bytes
	    return size




#HELPER CLASSES
# --------------------------------------------------------------------------------------------------------------------------------------------------------------- #

class fileWriter:
	"""This is write to file class """
	# --------------------------------------------------------------------------------------------------------------------------------------------------------------- #     
	def __init__(self, stdout, filename):
		self.stdout = stdout
		self.logfile = file(filename, 'a')
		
	# --------------------------------------------------------------------------------------------------------------------------------------------------------------- #     
	def write(self, text):
		self.stdout.write(text)
		self.logfile.write(text)
		
	# --------------------------------------------------------------------------------------------------------------------------------------------------------------- #  
	def close(self):
		self.stdout.close()
		self.logfile.close() 



#TODO: Automate the google mapping table by always getting current categories and insert any new ones. 
#TODO: Check product google_category with google_map value and if it's different from what the product has we need to update the product google_category


if __name__ == '__main__':
	#let's start the clock
	start_script = datetime.datetime.now()	
	#create class instance
	pf = productFeed()	
	
	print "###########################"
	print "## GENERIC PRODUCT FEED ##"
	print "###########################"
	print "Preparing resources for product feed build, please wait...."
	
	#Check for associated files
	pf.utlFileExists(pf.fileLog)
	pf.utlFileExists(pf.fileTmp)
	pf.utlFileExists(pf.fileOutProd)
	pf.utlFileExists(pf.fileOutExclusions)
	pf.utlFileExists(pf.fileOutCat)
	pf.utlFileExists(pf.fileTmp2)
	pf.utlFileExists(pf.fileNewCat)
	pf.utlFileExists(pf.fileWoCat)
	pf.utlFileExists(pf.fileReport)
		
	#Write header to product exclusion reports
	pf.csvWrite(['issue', 'pid', 'sku', 'title', 'brand', 'product_type', 'product_url', 'product_img_url', 'price', 'sale_price', 'qty', 'color', 'size', 'year'], pf.fileOutExclusions)
	pf.csvWrite(['manufacturer', 'color', 'description', 'product_img_url', 'product_url', 'price', 'sale_price', 'product_type', 'google_category', 'qty', 'size', 'gender'], self.fileReport)

	
	print "Getting products, please wait..."
	#Gets all product simples
	simplesList = pf.dbAllSimples("", "") # AND and LIMIT Sql params
		
	productCount = 0
	#Adds products to tmp
	for simples in simplesList:
		pf.csvWrite(simples[0:24], pf.fileTmp)
		print "loading --> Product: [sku:" + str(simples[1]) + "]"
		productCount = productCount + 1
		
	pf.log("Total Products Found " + str(productCount))

	#Writing GoToData	XML
	# --------------------------------------------------------------------------------------------------------------------------------------------------------------- #  
	#GoToData tmp.csv	
	reader = csv.reader(open(pf.fileTmp, "rb"))
	productParentList = []
	for row in reader:
		#this puts everything into a multi-dimensional list
		productParentList.append(row)
		
	goToData_start = datetime.datetime.now()
	pf.log("Started GotoData XML build (products.xml)")
	
	#start of xml file
	pf.out(pf.xmlGoToFirst(), pf.fileOutProd)
	pf.log(" -- GotoData XML build in progress, building " + str(productCount) + " products")
	#main of xml file
	productXmlTotal = pf.xmlProducts(productParentList, "gotodata")	
	#ending of xml file
	pf.out(pf.xmlGoToLast(productXmlTotal), pf.fileOutProd)

	goToData_end = datetime.datetime.now()	
	pf.log("Completed GotoData XML build (products.xml)")
	goToDataTotalTime = goToData_end - goToData_start
	pf.log("Total build time for products.xml: " + str(goToDataTotalTime))		
		
	#Writing Google XML
	# --------------------------------------------------------------------------------------------------------------------------------------------------------------- #  

	#Google generic_catalog.csv
	reader2 = csv.reader(open(pf.fileTmp2, "rb"))
	productParentList2 = []
	pcount = 0
	for row2 in reader2:
		productParentList2.append(row2)
		pcount = pcount + 1
	
	google_start = datetime.datetime.now()
	#pf.out("Starting with building Google XML (generic_catalog)", pf.fileLog)
	pf.log("Started Google XML build (generic_catalog)")
	
	#start of xml file
	pf.out(pf.xmlGoogleFirst(), pf.fileOutCat)	
	pf.log(" -- Google XML build in progress, building " + str(productCount) + " products")
	#main of xml file
	productXmlTotal = pf.xmlProducts(productParentList2, "google")	
	#ending of xml file
	pf.out(pf.xmlGoogleLast(), pf.fileOutCat)	

	pf.log("Completed Google XML build (generic_catalog)")
		
	google_end = datetime.datetime.now()
	googleTotalTime = google_end - google_start
	pf.log("Total build time for generic_catalog: " + str(googleTotalTime))

	#New categories
	# --------------------------------------------------------------------------------------------------------------------------------------------------------------- #  
	print "Generating new categories report, please wait... "
	categoriesDiffList = pf.dbGetCategoryDiff()
	#This write new categories report and return total count
	categoryCount = pf.productCategoryDiff(categoriesDiffList) -1
	pf.log("Completed New Categories CSV build")
	
	#Get products without categories
	# --------------------------------------------------------------------------------------------------------------------------------------------------------------- #  
	print "Generating products without categories report, please wait... "
	product_wo_cats = pf.dbProductMissingCategories()
	pcacount = 0
	for no_cats in product_wo_cats:
		pf.csvWrite(no_cats[0:24], pf.fileWoCat)
		pcacount = pcacount + 1	
	pf.log("Completed Products Without Categories CSV build")
	products_no_cats_count = str(pcacount)
		
	#Total script time end
	end_script = datetime.datetime.now()
	script_time_taken = end_script - start_script
	pf.log("Total build time: " + str(script_time_taken))
	

	#Move Google XML to web location for automatic fetching
	# --------------------------------------------------------------------------------------------------------------------------------------------------------------- # 
	shutil.copyfile(pf.fileOutCat, pf.fileWebDirectory)
	pf.log("Moving:" + pf.fileOutCat + " into " + pf.fileWebDirectory)
	print "Moving:" + pf.fileOutCat + " into " + pf.fileWebDirectory
		
	#Upload Files
	# --------------------------------------------------------------------------------------------------------------------------------------------------------------- #  
	if(pf.ftpEnabled):
		#Upload Google
		google_upload_time = datetime.datetime.now()
		pf.ftpUpload(pf.ftpGoogleHost, pf.ftpGoogleUser, pf.ftpGooglePass, pf.fileOutCat)
		#Upload GoToData
		gotodata_upload_time = datetime.datetime.now()
		pf.ftpUpload(pf.ftpGoToHost, pf.ftpGoToUser, pf.ftpGoToPass, pf.fileOutProd)

		
	#Create ZipFiles
	# --------------------------------------------------------------------------------------------------------------------------------------------------------------- #
	#TODO: Delete old zip files
	
	file_time = str(start_script.month) + "_" + str(start_script.day) + "_" + str(start_script.year)
	my_zip_file = "product_feed_"+file_time+".zip"
	pfeed = zipfile.ZipFile(my_zip_file, "w")
	fileList = [pf.fileOutCat, pf.fileOutProd, pf.fileOutExclusions, pf.fileNewCat, pf.fileLog, pf.fileWoCat, pf.fileReport]
	
	for name in fileList:
		pfeed.write(name, os.path.basename(name), zipfile.ZIP_DEFLATED)	
	
	pfeed.close()
	print "Zipping file and attaching to email"		
	email_attachment = str(pf.fileDirectory) + my_zip_file
	
	


	#Email Files and Reports
	# --------------------------------------------------------------------------------------------------------------------------------------------------------------- #  
	today = str(start_script.month) + "/" + str(start_script.day) + "/" + str(start_script.year)	
	exclusionsTotal = pf.fileLength(pf.fileOutExclusions) - 1
	
	#Read exclusions file
	reader = csv.reader(open(pf.fileOutExclusions, "rb"))
	productExclusionsList = []
	for row in reader:
		#this puts everything into a multi-dimensional list
		productExclusionsList.append(row)

	#Read new category file
	reader1 = csv.reader(open(pf.fileNewCat, "rb"))
	productCategoriesList = []
	for row1 in reader1:
		#this puts everything into a multi-dimensional list
		productCategoriesList.append(row1)
	
	email_subject = 'Generic Product Feed Report for: ' + today
		
	email_message = "#########################\n"
	email_message += "  GENERIC PRODUCT FEED \n"
	email_message += "#########################\n"
	email_message += "\n"
	email_message += "Today's Date: " + str(today)
	email_message += "\n"
	email_message += "\n"
	email_message += "TOTALS: \n"
	email_message += "-------------------------------------------------------\n"
	email_message += "Total Products: "+str(productCount) + "\n"
	email_message += "Total Exclusions: "+str(exclusionsTotal) + "\n"
	email_message += "Total New Categories: "+str(categoryCount) + " \n"
	email_message += "Total Products Without Categories: "+str(products_no_cats_count) + " \n"
	email_message += "\n"
	email_message += "TIMES: \n"
	email_message += "-------------------------------------------------------\n"
	email_message += "Started @ " + str(start_script) + "\n"
	email_message += "Finished @ " + str(end_script) + " \n"
	email_message += "Total Script Time: " + str(script_time_taken) + " \n"
	email_message += "\n"
	email_message += "Google XML Build Time: " + str(googleTotalTime) + " \n"
	email_message += "GoToData XML Build Time: " + str(goToDataTotalTime) + " \n"
	email_message += "GoToData uploaded the file feed @ " + str(gotodata_upload_time) + "\n"	
	email_message += "Google uploaded the file feed @ " + str(google_upload_time) + "\n"	
	email_message += "\n"
	email_message += "\n"
	email_message += "\n"
	email_message += "EXCLUSIONS DETAILS: \n"
	email_message += "-------------------------------------------------------\n"
	
	excount = 0
	#Get the first ten items
	for excu in productExclusionsList[:11]:
		if(excount != 0):
			email_message += "Exclusion: " + str(excount) + ") " + excu[0] + " -- sku: " + excu[2] + " -- title: "+ excu[3] +"\n"		
		else:
			if(len(productExclusionsList[:11]) == 1):
				email_message += "Congratulations -- no exclusions!"
			#break
		excount = excount + 1
		
	email_message += "\n"
	email_message += "\n"
	email_message += "\n"
	email_message += "\n"
	email_message += "NEW CATEGORY DETAILS: \n"
	email_message += "-------------------------------------------------------\n"
	
	catcount = 0
	for caty in productCategoriesList[:11]:
		if(catcount != 0):
			email_message += "New Category: " + str(catcount) + ") -- magento_id: " + caty[0] + " -- product_type: " + caty[1] +"\n"
		else:
			if(len(productCategoriesList[:11]) == 1):
				email_message += "Congratulations -- no new categories!"
		
		catcount = catcount + 1

	
	email_message += "\n\n\n\n"
	
	#Send email
	# --------------------------------------------------------------------------------------------------------------------------------------------------------------- #
	for email_user in pf.emailList:
		print "Sent email to: " + email_user
		pf.sendEmail(email_user, pf.fromEmail, email_subject, email_message, email_attachment)




	#Final output display
	print "Total script run-time: " + str(script_time_taken)



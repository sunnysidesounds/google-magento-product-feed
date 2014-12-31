## GOOGLE PRODUCT FEED FOR GENERIC

#### SCRIPT DESCRIPTION

The Google product feed builds a xml file with the products from the Magento database and uploads them to Google as well as gotodatafeed.com. (supported Magento versions 1.3, 1.10 and 1.12) 

#### SCRIPT EXECUTION (How to use it)

This script takes one argument and this argument is specific to what Magento version you are using.

Basic syntax: ```python product_feed.py <magneto_version>``` (Acceptable arugments --> 1.3, 1.10, 1.12)

Example: (To run version 1.12) ```python product_feed.py 1.12```


#### SCRIPT LOGIC OUTLINE

*	STEP1: Delete all old tmp / old files before starting (tmp.csv, tmp2.csv...etc)
*	STEP2: Runs the main query to get all active simples. 
*	STEP3: Does a check to make sure configurable is active and removes any simples where status is disabled. (This check was done due to a bug in 1.3)
*	STEP4: Off-loads all simples to a temporary csv file called tmp.csv
*	STEP5: Writes Gotodatafeed (products.xml) xml and tmp2.csv --> Time to complete 1 1/2 hours
*	STEP6: Takes the values from tmp2.csv and generates the the Google Feed (generic_catalog.xml) --> Time completed 16 seconds.
*	STEP7: Moves google XML to web location (This is in-case FTP uploading fails, Google can retrieve it from the web location)
*	STEP8: Upload files to Google and GoToFeed FTP Accounts
*	STEP9: Create zip file (products.xml, generic_catalog.xml, exclusions.csv, product_feed_log.txt, warming.csv)
*	STEP10: Emails zip files to email list. 

#### SCRIPT FILE DESCRIPTIONS

*	exclusions.csv --> This has all the products that have been excluded and why they were excluded from the feed. 
*	products.xml --> The xml file that is uploaded to GoToData
*	generic_catalog.xml --> This is the xml file uploaded to Google
*	feed_report.csv --> This is a csv version of the Google xml feed. 
*	warnings.csv --> These are all the products that have a minor issue that should be fixed but aren't excluded from the feed. 
*	new_categories.csv --> Any new categories that have been created but not mapped (DEPRECATED)
*	wo_categories.csv --> Any products without products (DEPRECATED)
*	tmp.csv --> Temporary file 
*	tmp2.csv --> Temporary file 
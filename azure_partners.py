import requests
import pymongo
from datetime import datetime

client = pymongo.MongoClient(
    "mongodb+srv://jeniferjasper165:8NuZuh79hoZqY44t@scraped-data.1hmas.mongodb.net/?retryWrites=true&w=majority&appName=Scraped-data",
    serverSelectionTimeoutMS=5000  
)
db = client["azure_partners_db"]
collection = db["azure_db"]

BASE_URL = "https://main.prod.marketplacepartnerdirectory.azure.com/api/partners"

COUNTRY_CODES = {"United States": "US", "United Kingdom": "GB"}  

def fetch_partners(page_offset, location, country_code):
    params = {
        "filter": f"products=Azure;sort=0;pageSize=18;onlyThisCountry=true;country={country_code};radius=100;locname={location};locationNotRequired=true;pageOffset={page_offset}"
    }
    response = requests.get(BASE_URL, params=params)
    if response.status_code == 200:
        return response.json().get("matchingPartners", {}).get("items", [])
    else:
        print(f"Failed to fetch data for {location} ({country_code}) with pageOffset {page_offset}")
        return []

def process_and_store(partner, location, country_code):
    company_id = partner.get("partnerId", "Not available")
    existing_entry = collection.find_one({"company_id": company_id})
    
    updated_fields = {}
    
    if existing_entry:
        for key in [
            "name", "description", "linkedInOrganizationProfile", "industryFocus", "logo", "product",
            "serviceType", "solutions", "competencies", "competenciesGold", "competenciesSilver",
            "solutionsEndorsements", "competencySummary", "referralPrograms", "programQualificationsMsp",
            "programQualificationsAsp", "solutionsPartnerDesignations"
        ]:
            if partner.get(key, "Not available") != existing_entry.get(key, "Not available"):
                updated_fields[key] = partner.get(key, "Not available")
    
        if updated_fields:
            collection.update_one(
                {"company_id": company_id},
                {"$set": {**updated_fields, "Last_modified": datetime.now(), "status": "updated"}}
            )
    else:
        data = {
            "company_id": company_id,
            "Name": partner.get("name", "Not available"),
            "Description": partner.get("description", "Not available"),
            "Linkedin": partner.get("linkedInOrganizationProfile", "Not available"),
            "Industry_focus": partner.get("industryFocus", "Not available"),
            "Logo": partner.get("logo", "Not available"),
            "Product": partner.get("product", "Not available"),
            "Service_type": partner.get("serviceType", "Not available"),
            "Solutions": partner.get("solutions", "Not available"),
            "Competencies": partner.get("competencies", "Not available"),
            "Competencies_gold": partner.get("competenciesGold", "Not available"),
            "Competencies_silver": partner.get("competenciesSilver", "Not available"),
            "Solution_endorsements": partner.get("solutionsEndorsements", "Not available"),
            "Competency_summary": partner.get("competencySummary", "Not available"),
            "Referral_program": partner.get("referralPrograms", "Not available"),
            "Program_qualifications_Msp": partner.get("programQualificationsMsp", "Not available"),
            "Program_qualifications_Asp": partner.get("programQualificationsAsp", "Not available"),
            "Solutions_partner_designations": partner.get("solutionsPartnerDesignations", "Not available"),
            "Locations": [location],
            "Last_modified": datetime.now(),
            "status": "active",
        }
        collection.insert_one(data)
        print(f"Stored: {data['Name']}")

def lambda_handler():
    for location, country_code in COUNTRY_CODES.items():
        for offset in range(0, 91, 18):
            partners = fetch_partners(offset, location, country_code)
            for partner in partners:
                process_and_store(partner, location, country_code)
    print("Data extraction and storage completed.")

lambda_handler()

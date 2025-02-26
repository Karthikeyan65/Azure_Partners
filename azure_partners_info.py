import requests
import pymongo
from datetime import datetime

client = pymongo.MongoClient("mongodb://localhost:27017/")  
db = client["Azure_partners_db"]
collection = db["eg"]

BASE_URL = "https://main.prod.marketplacepartnerdirectory.azure.com/api/partners"

COUNTRY_CODES = {
    "Algeria": "DZ", "Argentina": "AR", "Australia": "AU", "Austria": "AT", "Bangladesh": "BD", "Belgium": "BE", "Brazil": "BR",
    "Bulgaria": "BG", "Canada": "CA", "Chile": "CL", "China": "CN", "Colombia": "CO", "Croatia": "HR", "Czech Republic": "CZ",
    "Denmark": "DK", "Egypt": "EG", "Finland": "FI", "France": "FR", "Germany": "DE", "Greece": "GR", "Hong Kong": "HK",
    "Hungary": "HU", "India": "IN", "Indonesia": "ID", "Ireland": "IE", "Israel": "IL", "Italy": "IT", "Japan": "JP",
    "Kazakhstan": "KZ", "Kenya": "KE", "Malaysia": "MY", "Mexico": "MX", "Morocco": "MA", "Netherlands": "NL", "New Zealand": "NZ",
    "Nigeria": "NG", "Norway": "NO", "Pakistan": "PK", "Peru": "PE", "Philippines": "PH", "Poland": "PL", "Portugal": "PT",
    "Qatar": "QA", "Romania": "RO", "Saudi Arabia": "SA", "Serbia": "RS", "Singapore": "SG", "Slovakia": "SK", "South Africa": "ZA",
    "South Korea": "KR", "Spain": "ES", "Sweden": "SE", "Switzerland": "CH", "Thailand": "TH", "Tunisia": "TN", "Turkey": "TR",
    "Ukraine": "UA", "United Arab Emirates": "AE", "United Kingdom": "GB", "United States": "US", "Vietnam": "VN"
}

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
    if collection.find_one({"company_id": company_id, "Country_Code": country_code}):
        print(f"Skipping duplicate entry: {partner.get('name', 'Unknown')} in {location} ({country_code})")
        return
    
    data = {
        "company_id": company_id,
        "Name": partner.get("name", "Not available"),
        "Description": partner.get("description", "Not available"),
        "Linkedin": partner.get("linkedInOrganizationProfile", "Not available"),
        "Industry_focus": partner.get("industryFocus", "Not available"),
        "Logo": partner.get("logo", "Not available"),
        "Products": partner.get("product", "Not available"),
        "Services": partner.get("serviceType", "Not available"),
        "Solutions": partner.get("solutions", "Not available"),
        "Program_Qualifications": partner.get("programQualificationsMsp", "Not available"),
        "Competency_Summary": partner.get("competencySummary", "Not available"),
        "Target_company_sizes": partner.get("targetCustomerCompanySizes", "Not available"),
        "Location": location,
        "Country_Code": country_code,
        "Last_modified": datetime.now()
    }
    
    collection.update_one({"company_id": data["company_id"], "Country_Code": country_code}, {"$set": data}, upsert=True)
    print(f"Stored/Updated: {data['Name']} in {location} ({country_code})")

if __name__ == "__main__":
    processed_countries = set()
    
    for location, country_code in COUNTRY_CODES.items():
        if country_code in processed_countries:
            continue  
        processed_countries.add(country_code)
        
        for offset in range(0, 91, 18):  
            partners = fetch_partners(offset, location, country_code)
            for partner in partners:
                process_and_store(partner, location, country_code)
    
    print("Data extraction and storage completed.")

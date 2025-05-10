import requests
import pymongo
from datetime import datetime
import copy

client = pymongo.MongoClient(
    "mongodb+srv://jeniferjasper165:8NuZuh79hoZqY44t@scraped-data.1hmas.mongodb.net/?retryWrites=true&w=majority&appName=Scraped-data",
    serverSelectionTimeoutMS=5000
)
db = client["azure_partners_db"]
collection = db["azure_info"]

BASE_URL = "https://main.prod.marketplacepartnerdirectory.azure.com/api/partners"

COUNTRY_CODES = {
    "Algeria": "DZ",
    "Argentina": "AR",
    "Australia": "AU",
    "Austria": "AT",
    "Bangladesh": "BD",
    "Belgium": "BE",
    "Brazil": "BR",
    "Bulgaria": "BG",
    "Canada": "CA",
    "Chile": "CL",
    "China": "CN",
    "Colombia": "CO",
    "Croatia": "HR",
    "Czech Republic": "CZ",
    "Denmark": "DK",
    "Egypt": "EG",
    "Finland": "FI",
    "France": "FR",
    "Germany": "DE",
    "Greece": "GR",
    "Hong Kong": "HK",
    "Hungary": "HU",
    "India": "IN",
    "Indonesia": "ID",
    "Ireland": "IE",
    "Israel": "IL",
    "Italy": "IT",
    "Japan": "JP",
    "Kazakhstan": "KZ",
    "Kenya": "KE",
    "Malaysia": "MY",
    "Mexico": "MX",
    "Morocco": "MA",
    "Netherlands": "NL",
    "New Zealand": "NZ",
    "Nigeria": "NG",
    "Norway": "NO",
    "Pakistan": "PK",
    "Peru": "PE",
    "Philippines": "PH",
    "Poland": "PL",
    "Portugal": "PT",
    "Qatar": "QA",
    "Romania": "RO",
    "Saudi Arabia": "SA",
    "Serbia": "RS",
    "Singapore": "SG",
    "Slovakia": "SK",
    "South Africa": "ZA",
    "South Korea": "KR",
    "Spain": "ES",
    "Sweden": "SE",
    "Switzerland": "CH",
    "Thailand": "TH",
    "Tunisia": "TN",
    "Turkey": "TR",
    "Ukraine": "UA",
    "United Arab Emirates": "AE",
    "United Kingdom": "GB",
    "United States": "US",
    "Vietnam": "VN",
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
    active_entry = collection.find_one({"company_id": company_id, "status": "active"})

    new_data = {
        "Company_id": company_id,
        "Name": partner.get("name", "Not available"),
        "Description": partner.get("description", "Not available"),
        "Linkedin": partner.get("linkedInOrganizationProfile", "Not available"),
        "Industry_focus": partner.get("industryFocus", "Not available"),
        "Logo": partner.get("logo", "Not available"),
        "Product": partner.get("product", "Not available"),
        "Service_type": partner.get("serviceType", "Not available"),
        "Solutions": partner.get("solutions", "Not available"),
        "Program_qualifications_Msp": partner.get("programQualificationsMsp", "Not available"),
        "Program_qualifications_Asp": partner.get("programQualificationsAsp", "Not available"),
        "Competencies": partner.get("competencies", "Not available"),
        "Competencies_gold": partner.get("competenciesGold", "Not available"),
        "Competencies_silver": partner.get("competenciesSilver", "Not available"),
        "Solutions_partner_designations": partner.get("solutionsPartnerDesignations", "Not available"),
        "Competency_summary": partner.get("competencySummary", "Not available"),
        "Locations": [location],
        "Last_modified": datetime.now(),
    }

    if active_entry:
        updated = False
        previous_data = copy.deepcopy(active_entry)
        new_data["locations"] = list(set(active_entry.get("locations", []) + [location]))

        for key in new_data:
            if key in ["_id", "last_modified", "status", "counter"]:
                continue
            if new_data[key] != active_entry.get(key):
                updated = True
                break

        # If only location is new, update the same document
        if not updated and new_data["locations"] != active_entry.get("locations", []):
            collection.update_one(
                {"_id": active_entry["_id"]},
                {
                    "$set": {
                        "locations": new_data["locations"],
                        "last_modified": datetime.now()
                    }
                }
            )
            print(f"Location updated for: {new_data['name']}")
            return

        if updated:
            # Remove status from old document
            collection.update_one(
                {"_id": active_entry["_id"]},
                {"$unset": {"status": ""}}
            )

            new_data["counter"] = active_entry.get("counter", 1) + 1
            new_data["status"] = "active"
            collection.insert_one(new_data)
            print(f"Updated: {new_data['name']} - Version {new_data['counter']}")
        else:
            print(f"No change for: {new_data['name']}")

    else:
        # First insert
        new_data["counter"] = 1
        collection.insert_one(new_data)  # no status field
        print(f"Stored: {new_data['name']} in {location} ({country_code})")


def lambda_handler():
    for location, country_code in COUNTRY_CODES.items():
        for offset in range(0, 91, 18):
            partners = fetch_partners(offset, location, country_code)
            for partner in partners:
                process_and_store(partner, location, country_code)
    print("Data extraction and storage completed.")


lambda_handler()

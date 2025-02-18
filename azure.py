import asyncio
import json
from playwright.async_api import async_playwright
import requests
from pymongo import MongoClient
from datetime import datetime

# Constants
API_URL = "https://main.prod.marketplacepartnerdirectory.azure.com/api/partners"
DETAILS_URL = "https://main.prod.marketplacepartnerdirectory.azure.com/api/partners/{company_id}"
PAGE_SIZE = 18
RETRY_DELAY = 30
TIMEOUT = 60 * 1000
MAX_RETRIES = 5

# MongoDB setup
MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "Azure_partners_db"
COLLECTION_NAME = "Azure_db"

class AzurePartnerScraper:
    def __init__(self):
        self.client = MongoClient(MONGO_URI)
        self.db = self.client[DB_NAME]
        self.collection = self.db[COLLECTION_NAME]
        self.processed_count = 0

    async def fetch_batch(self, request_context, page_offset):
        """Fetch a batch of company IDs"""
        url = f"{API_URL}?filter=sort%3D0%3BpageSize%3D{PAGE_SIZE}&pageOffset={page_offset}"
        
        for attempt in range(MAX_RETRIES):
            try:
                response = await request_context.get(url, timeout=TIMEOUT)
                if response.status == 200:
                    data = await response.json()
                    # Extract partner IDs from the response
                    company_ids = [item['partnerId'] for item in data.get('matchingPartners', {}).get('items', [])]
                    return company_ids
                print(f"Error {response.status} - Attempt {attempt + 1}/{MAX_RETRIES}")
            except Exception as e:
                print(f"Error fetching batch: {e} - Attempt {attempt + 1}/{MAX_RETRIES}")
            await asyncio.sleep(RETRY_DELAY)
        return []

    def process_partner_data(self, data):
        """Process and clean partner data for storage"""
        if not data:
            return None

        processed_data = {
            "company_id": data.get("partnerDetails", {}).get("id"),
            "name": data.get("partnerDetails", {}).get("name"),
            "description": data.get("partnerDetails", {}).get("description"),
            "website": data.get("partnerDetails", {}).get("url"),
            "linkedin": data.get("partnerDetails", {}).get("linkedInOrganizationProfile"),
            "logo": data.get("partnerDetails", {}).get("logo"),
            "industry_focus": data.get("partnerDetails", {}).get("industryFocus", []),
            "products": data.get("partnerDetails", {}).get("product", []),
            "services": data.get("partnerDetails", {}).get("serviceType", []),
            "solutions": data.get("partnerDetails", {}).get("solutions", []),
            "target_company_sizes": data.get("partnerDetails", {}).get("targetCustomerCompanySizes", []),
            "last_updated": datetime.now()  # Store the current datetime
        }
        return processed_data

    async def fetch_company_details(self, company_id):
        """Fetch detailed information for a company"""
        response = requests.get(
            DETAILS_URL.format(company_id=company_id),
            timeout=30
        )
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error fetching details for company {company_id}: {response.status_code}")
        return None

    async def process_batch(self, company_ids):
        """Process a batch of company IDs and store their details in the DB"""
        for company_id in company_ids:
            try:
                # Fetch company details
                partner_data = await self.fetch_company_details(company_id)
                if partner_data:
                    # Process and insert the data into MongoDB
                    processed_data = self.process_partner_data(partner_data)
                    if processed_data:
                        # Insert or update the partner data in MongoDB
                        self.collection.update_one(
                            {"company_id": processed_data["company_id"]},
                            {"$set": processed_data},
                            upsert=True  # If the company_id doesn't exist, insert it
                        )
                        self.processed_count += 1
                        print(f"Processed and stored data for company {processed_data['name']}")
            except Exception as e:
                print(f"Error processing company {company_id}: {e}")
            await asyncio.sleep(1)  # Rate limiting

    async def run(self):
        """Main execution method"""
        print("Starting Azure Partner Directory scraping...")
        async with async_playwright() as p:
            request_context = await p.request.new_context()
            page_offset = 0
            
            while True:
                print(f"Fetching batch at offset {page_offset}...")
                company_ids = await self.fetch_batch(request_context, page_offset)
                
                if not company_ids:
                    print("No more companies to process")
                    break
                
                print(f"Processing batch of {len(company_ids)} companies...")
                await self.process_batch(company_ids)
                
                print(f"Completed batch. Total processed: {self.processed_count}")
                page_offset += PAGE_SIZE

        print(f"Scraping completed. Total partners processed: {self.processed_count}")

if __name__ == "__main__":
    scraper = AzurePartnerScraper()
    asyncio.run(scraper.run())

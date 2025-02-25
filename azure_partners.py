import asyncio
import string
import os
import json
from playwright.async_api import async_playwright
from pymongo import MongoClient
from datetime import datetime

# Constants
API_URL = "https://appsource.microsoft.com/en-us/marketplace/partner-dir"
DETAILS_URL = "https://main.prod.marketplacepartnerdirectory.azure.com/api/partners/{company_id}"
PAGE_SIZE = 18
RETRY_DELAY = 30
TIMEOUT = 60 * 1000
MAX_RETRIES = 5
TEMP_FILE = "temp_partners.txt"

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
        self.processed_ids = self.load_processed_ids()

    def load_processed_ids(self):
        """Load previously processed partner IDs from a text file"""
        if os.path.exists(TEMP_FILE):
            with open(TEMP_FILE, "r") as file:
                return set(line.strip() for line in file)
        return set()

    def save_processed_id(self, company_id):
        """Save a processed partner ID to a text file"""
        with open(TEMP_FILE, "a") as file:
            file.write(f"{company_id}\n")
        self.processed_ids.add(company_id)

    async def fetch_batch(self, request_context, page_offset, search_letter):
        """Fetch a batch of company IDs using Playwright"""
        url = f"{API_URL}?filter=sort%3D0%3BpageSize%3D18%3BpageOffset%3D{page_offset}%3Bradius%3D100%3Bfreetext%3D{search_letter}%3Bsuggestion%3Dtrue%3BlocationNotRequired%3Dtrue"

        for attempt in range(MAX_RETRIES):
            try:
                response = await request_context.get(url, timeout=TIMEOUT)
                print("Response:::::", response)
                if response.status == 200:
                    data = await response.json()
                    
                    company_ids = [item['partnerId'] for item in data.get('matchingPartners', {}).get('items', [])]
                    print("Compnay_ids::::", company_ids)
                    return company_ids
                print(f"Error {response.status} - Attempt {attempt + 1}/{MAX_RETRIES}")
            except Exception as e:
                print(f"Error fetching batch: {e} - Attempt {attempt + 1}/{MAX_RETRIES}")
            await asyncio.sleep(RETRY_DELAY)
        return []

    async def fetch_company_details(self, request_context, company_id):
        """Fetch detailed information for a company using Playwright"""
        url = DETAILS_URL.format(company_id=company_id)

        for attempt in range(MAX_RETRIES):
            try:
                response = await request_context.get(url, timeout=TIMEOUT)
                if response.status == 200:
                    return await response.json()
                print(f"Error {response.status} fetching details for {company_id} - Attempt {attempt + 1}/{MAX_RETRIES}")
            except Exception as e:
                print(f"Error fetching details for {company_id}: {e} - Attempt {attempt + 1}/{MAX_RETRIES}")
            await asyncio.sleep(RETRY_DELAY)
        return None

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
            "last_updated": datetime.now()
        }
        return processed_data

    async def process_batch(self, request_context, company_ids):
        """Process a batch of company IDs and store their details in the DB"""
        for company_id in company_ids:
            if company_id in self.processed_ids:
                print(f"Skipping already processed company: {company_id}")
                continue

            try:
                partner_data = await self.fetch_company_details(request_context, company_id)
                if partner_data:
                    processed_data = self.process_partner_data(partner_data)
                    if processed_data:
                        self.collection.update_one(
                            {"company_id": processed_data["company_id"]},
                            {"$set": processed_data},
                            upsert=True
                        )
                        self.processed_count += 1
                        print(f"Processed and stored data for company {processed_data['name']}")

                        self.save_processed_id(company_id)
            except Exception as e:
                print(f"Error processing company {company_id}: {e}")
            await asyncio.sleep(1)

    async def run(self):
        """Main execution method"""
        print("Starting Azure Partner Directory scraping...")

        async with async_playwright() as p:
            # Launch Chromium browser
            browser = await p.chromium.launch(headless=False)
            context = await browser.new_context()
            request_context = await p.request.new_context()  # ✅ Fixed request context initialization

            # Iterate through all alphabets a-z in freetext filter
            for letter in string.ascii_lowercase:
                page_offset = 18
                click_count = 0

                while True:
                    print(f"Fetching batch for letter '{letter}' at offset {page_offset}...")
                    company_ids = await self.fetch_batch(request_context, page_offset, letter)

                    if not company_ids:
                        print(f"No more companies found for letter '{letter}', moving to next letter.")
                        break

                    click_count += 1
                    if click_count % 10 == 0:
                        print(f"Processing {len(company_ids)} companies for letter '{letter}' after {click_count} clicks...")
                        await self.process_batch(request_context, company_ids)

                    print(f"Completed batch for letter '{letter}', offset {page_offset}. Total processed: {self.processed_count}")
                    page_offset += PAGE_SIZE

            await browser.close()

        print(f"Scraping completed. Total partners processed: {self.processed_count}")

if __name__ == "__main__":
    scraper = AzurePartnerScraper()
    asyncio.run(scraper.run())

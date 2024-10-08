import pandas as pd
from datetime import datetime
from linkedin_api import Linkedin
from serpapi import GoogleSearch
import json

# LinkedIn and SerpAPI credentials
USERNAME = "test"
PASSWORD = "test"
SERPAPI_KEY = "test"

# Load the existing CSV file
file_path = 'linkedinData.csv'
df = pd.read_csv(file_path)

# Extract user names from the Informal Name column
user_names = df['Informal Name'].dropna().tolist()

# Authenticate using LinkedIn credentials
try:
    api = Linkedin(USERNAME, PASSWORD)
    print("Successfully authenticated!\n")
    search_results = api.search_people(keywords="Pranav Raveendran", keyword_school='University of Maryland')
    print(search_results)
except Exception as e:
    print(f"Authentication failed: {e}")
    exit()

# Function to fetch LinkedIn profile URL using SerpAPI
def fetch_linkedin_profile_url(user_name):
    try:
        params = {
            "engine": "google",
            "q": f"site:linkedin.com/in {user_name} University of Maryland",
            "api_key": SERPAPI_KEY,
            "num": 1
        }
        search = GoogleSearch(params)
        results = search.get_dict()

        if results and 'organic_results' in results:
            for result in results['organic_results']:
                if "linkedin.com/in" in result.get('link', ''):
                    return result['link']
        return None
    except Exception as e:
        print(f"Error fetching LinkedIn URL for {user_name}: {e}")
        return None

# Function to fetch and process user data
def fetch_user_data(user_name):
    try:
        # First, try to fetch LinkedIn profile URL using SerpAPI
        linkedin_url = fetch_linkedin_profile_url(user_name)
        public_id = None

        if linkedin_url:
            public_id = linkedin_url.split('/in/')[-1].strip('/')
            print(public_id)
        else:
            
            # If no LinkedIn URL from SerpAPI, fall back to LinkedIn API search
            search_results = api.search_people(keywords=user_name, keyword_school='University of Maryland')
            if not search_results:
                return None
            public_id = search_results[0].get('public_id')
            if not public_id:
                return None
        # Fetch detailed profile information using the public_id
        profile_data = api.get_profile(public_id)
        if not profile_data or not isinstance(profile_data, dict):
            print(f"Error: Empty or invalid profile data for user {user_name}.")
            return None

        date_of_lookup = datetime.now().strftime("%Y-%m-%d")

        # Check if the person attended the University of Maryland
        attended_umd = any(
            "university of maryland" in edu.get('schoolName', '').lower()
            for edu in profile_data.get('education', [])
        )
        if not attended_umd:
            return None

        # Extract employment details
        current_employer_name, current_job_title = "Currently Unemployed", "N/A"
        employment_city, employment_state, employment_country = "N/A", "N/A", "N/A"
        for exp in profile_data.get('experience', []):
            if 'timePeriod' in exp and 'endDate' not in exp['timePeriod']:
                current_employer_name = exp.get('companyName', 'Unknown')
                current_job_title = exp.get('title', 'Unknown')
                location = exp.get('locationName', 'Unknown').split(", ")
                employment_city, employment_state, employment_country = (location + ["N/A", "N/A"])[:3]
                break

        # Extract education details
        currently_pursuing_education, school_name, degree_name, field_of_study = "No", "N/A", "N/A", "N/A"
        for edu in profile_data.get('education', []):
            if 'timePeriod' in edu and 'endDate' in edu['timePeriod']:
                end_year = edu['timePeriod']['endDate'].get('year', 0)
                end_month = edu['timePeriod']['endDate'].get('month', 12)
                end_date = datetime(end_year, end_month, 1)
                if end_date > datetime.now():
                    currently_pursuing_education = "Yes"
                    school_name = edu.get('schoolName', 'Unknown')
                    degree_name = edu.get('degreeName', 'Unknown')
                    field_of_study = edu.get('fieldOfStudy', 'Unknown')
                    break

        return {
            'Informal Name': user_name, 'Date of Lookup': date_of_lookup, 'LinkedIn URL': linkedin_url,
            'Current Employer Name': current_employer_name, 'Job Title': current_job_title,
            'Employment City': employment_city, 'Employment State': employment_state,
            'Employment Country': employment_country, 'Currently Pursuing Education': currently_pursuing_education,
            'School Name': school_name, 'Degree Name': degree_name, 'Field of Study': field_of_study,
            'Attended University of Maryland': 'Yes'
        }
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON for user {user_name}. Skipping...")
        print(e)
        return None
    except Exception as e:
        print(f"Error fetching data for user {user_name}: {e}")
        return None

# Process each user synchronously
for user_name in user_names:
    print(f"Processing: {user_name}")
    user_data = fetch_user_data(user_name)
    if user_data:
        # Update the corresponding row in the DataFrame
        df.loc[df['Informal Name'] == user_name, list(user_data.keys())] = list(user_data.values())
        print(f"Data fetched and updated for {user_name}")
    else:
        print(f"Data not found or failed to fetch for {user_name}.")

# Save the updated DataFrame back to the CSV file
df.to_csv(file_path, index=False)
print(f"Results appended to {file_path}")

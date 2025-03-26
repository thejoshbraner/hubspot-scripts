#!/usr/bin/env python3
import csv
import os
import requests
import logging
import sys
from dotenv import load_dotenv
import re
import colorlog


load_dotenv()  # Load environment variables from .env

# Configure logging: log both to console and file.
# Create a file handler for logging to file.
file_handler = logging.FileHandler("hubspot_property_import.log")
file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))

# Create a colored console handler.
console_handler = colorlog.StreamHandler(sys.stdout)
console_handler.setFormatter(colorlog.ColoredFormatter(
    '%(log_color)s%(asctime)s - %(levelname)s - %(message)s',
    log_colors={
        'DEBUG': 'cyan',
        'INFO': 'green',
        'WARNING': 'yellow',
        'ERROR': 'red',
        'CRITICAL': 'bold_red',
    }
))

# Get the root logger and add both handlers.
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(file_handler)
logger.addHandler(console_handler)

# Updated mapping: Allowed types are one of [string, enumeration, number, bool, datetime, date, phone_number]
TYPE_MAPPING = {
    "Text": {"type": "string", "fieldType": "text"},
    "Single-line Text": {"type": "string", "fieldType": "text"},
    "Multi-line Text": {"type": "string", "fieldType": "textarea"},
    "Number": {"type": "number", "fieldType": "number"},
    "Currency Number": {"type": "number", "fieldType": "number"},
    "Dropdown": {"type": "enumeration", "fieldType": "select"},
    "Multiple Checkboxes": {"type": "enumeration", "fieldType": "select", "multiple": True},
    "Unformatted Number": {"type": "number", "fieldType": "number"},
    "Single Checkbox": {"type": "bool", "fieldType": "booleancheckbox"},
    "HubSpot User": {"type": "string", "fieldType": "text"},
    "Date Picker": {"type": "date", "fieldType": "date"}
}

# Map our CSV object type names to HubSpot API endpoints.
OBJECT_TYPE_MAPPING = {
    "Contact": "contacts",
    "Company": "companies",
    "Deal": "deals"
}

# Group details
GROUP_ID = "api_imported_properties"
GROUP_DISPLAY_NAME = "ATI Seminars Properties - API"

# Base URL for HubSpot CRM Properties API
BASE_URL = "https://api.hubapi.com/crm/v3/properties"

# Get HubSpot OAuth access token from environment variable
ACCESS_TOKEN = os.environ.get("HUBSPOT_ACCESS_TOKEN")
if not ACCESS_TOKEN:
    logging.error("HUBSPOT_ACCESS_TOKEN environment variable not set. Exiting.")
    sys.exit(1)

HEADERS = {
    "Authorization": f"Bearer {ACCESS_TOKEN}",
    "Content-Type": "application/json"
}

def generate_api_property_name(original_name):
    """
    Convert the provided property name to a valid HubSpot API property name:
    - Lowercase
    - Replace spaces and non-alphanumeric characters with underscores
    """
    cleaned = re.sub(r'[^A-Za-z0-9 ]+', '', original_name)
    return cleaned.strip().lower().replace(" ", "_")

def ensure_property_group(object_type_api, group_id, display_name):
    """
    Check if a property group exists for a given object type. If not, create it.
    """
    group_url = f"{BASE_URL}/{object_type_api}/groups"
    try:
        response = requests.get(group_url, headers=HEADERS)
        if response.status_code == 200:
            groups = response.json().get("results", [])
            for group in groups:
                if group.get("name") == group_id:
                    return True
            # If not found, create it.
            payload = {
                "name": group_id,
                "label": display_name,   # Use "label" as required by the API
                "displayOrder": 1
            }
            create_resp = requests.post(group_url, json=payload, headers=HEADERS)
            if create_resp.status_code in [200, 201]:
                logging.info(f"Created property group '{display_name}' for {object_type_api}.")
                return True
            else:
                logging.error(f"Failed to create property group for {object_type_api}: {create_resp.status_code} - {create_resp.text}")
                return False
        else:
            logging.error(f"Failed to fetch property groups for {object_type_api}: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        logging.error(f"Exception while ensuring property group for {object_type_api}: {e}")
        return False

def property_exists(object_type_api, property_name):
    """
    Check if a property exists on a given HubSpot object type.
    """
    url = f"{BASE_URL}/{object_type_api}/{property_name}"
    response = requests.get(url, headers=HEADERS)
    if response.status_code == 200:
        return True
    elif response.status_code == 404:
        return False
    else:
        logging.error(f"Unexpected response checking property '{property_name}' on '{object_type_api}': {response.status_code} - {response.text}")
        return None

def create_property(object_type_api, payload):
    """
    Create a property for a given HubSpot object type.
    """
    url = f"{BASE_URL}/{object_type_api}"
    response = requests.post(url, json=payload, headers=HEADERS)
    return response

def process_csv(file_path):
    created = []
    skipped = []
    errors = []

    # Keep track of which object types we've ensured the property group for.
    group_checked = {}

    with open(file_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            # Expected CSV columns:
            # "Property Name", "Property Type", "Property Options", "Object Type"
            original_prop_name = row["Property Name"].strip()
            prop_type_raw = row["Property Type"].strip()
            prop_options_raw = row["Property Options"].strip()
            object_type_raw = row["Object Type"].strip()

            # Map CSV object type to API object type; if not found in our mapping, use the CSV value directly.
            object_type_api = OBJECT_TYPE_MAPPING.get(object_type_raw, object_type_raw)
            if not object_type_api:
                logging.error(f"Unknown object type '{object_type_raw}' for property '{original_prop_name}'. Skipping.")
                errors.append(original_prop_name)
                continue

            # Ensure property group exists for this object type (only once per type)
            if object_type_api not in group_checked:
                if not ensure_property_group(object_type_api, GROUP_ID, GROUP_DISPLAY_NAME):
                    logging.error(f"Cannot ensure property group for '{object_type_api}'. Skipping properties for this object type.")
                    group_checked[object_type_api] = False
                else:
                    group_checked[object_type_api] = True
            if not group_checked[object_type_api]:
                errors.append(original_prop_name)
                continue

            # Map the CSV property type to API required type and fieldType.
            if prop_type_raw not in TYPE_MAPPING:
                logging.error(f"Unknown property type '{prop_type_raw}' for property '{original_prop_name}'. Skipping.")
                errors.append(original_prop_name)
                continue

            mapping = TYPE_MAPPING[prop_type_raw]
            api_type = mapping["type"]
            field_type = mapping["fieldType"]

            # Generate a valid property name (must be lowercase with no spaces)
            api_prop_name = generate_api_property_name(original_prop_name)

            # Build the payload.
            payload = {
                "name": api_prop_name,
                "label": original_prop_name,
                "groupName": GROUP_ID,
                "type": api_type,
                "fieldType": field_type
            }
            if mapping.get("multiple"):
                payload["multiple"] = True

            # Process property options only for property types that support them.
            if prop_type_raw in ["Dropdown", "Multiple Checkboxes", "Single Checkbox"] and prop_options_raw:
                options_list = [opt.strip() for opt in prop_options_raw.split(",") if opt.strip()]
                options = []
                for opt in options_list:
                    value = opt.lower().replace(" ", "_")
                    options.append({"label": opt, "value": value})
                payload["options"] = options

            # Check if property already exists.
            exists = property_exists(object_type_api, api_prop_name)
            if exists is None:
                logging.error(f"Error checking existence for property '{original_prop_name}' (internal name '{api_prop_name}') on '{object_type_api}'. Skipping.")
                errors.append(original_prop_name)
                continue
            elif exists:
                logging.info(f"Property '{original_prop_name}' (internal name '{api_prop_name}') already exists on '{object_type_api}'. Skipping.")
                skipped.append(original_prop_name)
                continue

            # Create the property.
            response = create_property(object_type_api, payload)
            if response.status_code in [200, 201]:
                logging.info(f"Successfully created property '{original_prop_name}' (internal name '{api_prop_name}') on '{object_type_api}'.")
                created.append(original_prop_name)
            else:
                # If the error is due to non-unique property label, treat as skipped.
                try:
                    error_json = response.json()
                    if error_json.get("subCategory") == "PropertyValidationError.NON_UNIQUE_PROPERTY_LABEL":
                        logging.info(f"Property '{original_prop_name}' (internal name '{api_prop_name}') already exists (non-unique label). Skipping.")
                        skipped.append(original_prop_name)
                        continue
                except Exception:
                    pass
                logging.error(f"Failed to create property '{original_prop_name}' (internal name '{api_prop_name}') on '{object_type_api}': {response.status_code} - {response.text}")
                errors.append(original_prop_name)

    # Log summary.
    logging.info("========== Summary ==========")
    logging.info(f"Properties Created: {created}")
    logging.info(f"Properties Skipped (already exist): {skipped}")
    logging.info(f"Properties with Errors: {errors}")

def main():
    # File path for the CSV (adjust path if needed)
    csv_file_path = "./properties.csv"
    process_csv(csv_file_path)

if __name__ == "__main__":
    main()

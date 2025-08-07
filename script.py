import pandas as pd
import requests
import json

# -------------------------
# INPUT / OUTPUT FILES
# -------------------------
input_file1 = 'bgbase.csv'
input_file2 = 'gbif.csv'
output_file = 'output.csv'

# -------------------------
# READ INPUT FILES
# -------------------------
df1 = pd.read_csv(input_file1, encoding='ISO-8859-1')
df2 = pd.read_csv(input_file2, encoding='ISO-8859-1')

df1_extracted = pd.DataFrame({
    'scientificName': df1['SCIENTIFIC_NAME'].astype(str).str.strip(),
    'sourceFile': input_file1,
    'powoName': ''
})

df2_extracted = pd.DataFrame({
    'scientificName': df2['verbatimScientificName'].astype(str).str.strip(),
    'sourceFile': input_file2,
    'powoName': ''
})

# Combine full dataset (with duplicates)
combined_df = pd.concat([df1_extracted, df2_extracted], ignore_index=True)

# -------------------------
# PREP FOR BATCH POST TO GNV
# -------------------------
names_list = combined_df['scientificName'].tolist()

verifier_api_url = "https://verifier.globalnames.org/api/v1/verifications"
params = {
    "nameStrings": names_list,
    "withAllMatches": True,
    "withCapitalization": True,
    "withSpeciesGroup": True,
    "withUninomialFuzzyMatch": False,
    "withStats": True,
    "mainTaxonThreshold": 0.6,
    "preferredSources": [197]  # POWO only
}

# -------------------------
# SEND REQUEST TO GNV
# -------------------------
print("🔄 Sending batch request to Global Names Verifier...")
response = requests.post(verifier_api_url, json=params, timeout=120)
response.raise_for_status()
results_json = response.json()
print("✅ Received response from GNV")

# -------------------------
# PARSE GNV RESULTS BY POSITION
# -------------------------
gnv_data = []
for item in results_json["names"]:
    name = item.get("suppliedInput", "")
    match_type = item.get("matchType", "")
    matched = {}

    # Prefer POWO result
    for result in item.get("results", []):
        if result.get("dataSourceId") == 197:
            matched = result
            break

    # Fallback to first available
    if not matched and item.get("results"):
        matched = item["results"][0]

    gnv_data.append({
        'GNVmatchType': match_type,
        'GNVmatchedCanonicalFull': matched.get('matchedCanonicalFull', ''),
        'GNVisSynonym': matched.get('isSynonym', ''),
        'GNVcurrentCanonicalFull': matched.get('currentCanonicalFull', ''),
        'GNVdataSourceTitleShort': matched.get('dataSourceTitleShort', '')
    })

# -------------------------
# APPEND RESULTS BY POSITION
# -------------------------
gnv_df = pd.DataFrame(gnv_data)
final_df = pd.concat([combined_df.reset_index(drop=True), gnv_df.reset_index(drop=True)], axis=1)

# -------------------------
# WRITE TO OUTPUT
# -------------------------
final_df.to_csv(output_file, index=False)
print(f"✅ Output written to {output_file}")

import boto3
import io
from flask import Flask, jsonify
from flask_cors import CORS
import pandas as pd
import json

app = Flask(__name__)
CORS(app)

def load_data():
    s3 = boto3.client('s3')
    bucket_name = 'chakus3bucket'
    key = 'MVP/MVP_Dataframe.csv'

    response = s3.get_object(Bucket=bucket_name, Key=key)
    csv_content = response['Body'].read().decode('utf-8')

    df = pd.read_csv(io.StringIO(csv_content))
    
    # Clean up duplicate entries and create unique IDs
    df['farmer_id'] = df.groupby(['first_name', 'last_name']).ngroup()
    df['farm_id'] = df.index
    return df

@app.route('/farmers')
def get_farmers():
    df = load_data()
    # Get unique farmers with all required fields
    farmers = df.drop_duplicates(subset=['first_name', 'last_name']).copy()
    
    # Create a clean data structure for each farmer
    farmers_data = []
    for _, farmer in farmers.iterrows():
        farmer_data = {
            'farmer_id': int(farmer['farmer_id']),
            'first_name': str(farmer['first_name']).strip(),
            'last_name': str(farmer['last_name']).strip(),
            'farmer_image_url': str(farmer['farmer_image_url']) if pd.notna(farmer['farmer_image_url']) else None,
            'nationality': str(farmer['nationality']).strip(),
            'farm_location': str(farmer['farm_location']).strip(),
            'crop_name': str(farmer['crop_name']).strip(),
            'variety': str(farmer['variety']).strip() if pd.notna(farmer['variety']) else ''
        }
        farmers_data.append(farmer_data)
    
    print(f"Returning {len(farmers_data)} farmers")  # Debug print
    return jsonify(farmers_data)

def parse_coordinates(coord_str):
    try:
        if pd.isna(coord_str):
            return {"latitude": 6.11000000, "longitude": -0.11000000}
        
        # Split the string into pairs of coordinates
        coords = coord_str.strip().split(',')[0].split()
        if len(coords) == 2:
            return {
                "latitude": float(coords[0]),
                "longitude": float(coords[1])
            }
        return {"latitude": 6.11000000, "longitude": -0.11000000}
    except Exception as e:
        print(f"Error parsing coordinates: {e}, Original value: {coord_str}")
        return {"latitude": 6.11000000, "longitude": -0.11000000}

def parse_boundaries(bound_str):
    try:
        if pd.isna(bound_str):
            return []
            
        # Split the string into coordinate pairs
        pairs = bound_str.strip().split(',')
        boundaries = []
        
        for pair in pairs:
            if pair and pair.strip() != 'NA':
                coords = pair.strip().split()
                if len(coords) == 2:
                    boundaries.append({
                        "latitude": float(coords[0]),
                        "longitude": float(coords[1])
                    })
        
        # Only return boundaries if we have at least 3 points to make a polygon
        return boundaries if len(boundaries) >= 3 else []
        
    except Exception as e:
        print(f"Error parsing boundaries: {e}, Original value: {bound_str}")
        return []

@app.route('/farms')
def get_farms():
    df = load_data()
    farms = df[['farm_id', 'farmer_id', 'first_name', 'last_name', 
                'farm_number', 'farm_location', 'crop_name', 'variety',
                'geo_coordinates', 'geo_boundaries', 'acreage'
    ]].copy()
    
    # Fill missing values
    farms = farms.fillna({
        'farm_number': '',
        'farm_location': 'Location not specified',
        'crop_name': '',
        'variety': '',
        'geo_coordinates': '',
        'geo_boundaries': '', 
        'acreage':''
    })

    # Parse coordinates and boundaries
    farms['geo_coordinates'] = farms['geo_coordinates'].apply(parse_coordinates)
    farms['geo_boundaries'] = farms['geo_boundaries'].apply(parse_boundaries)

    farms_list = farms.to_dict('records')
    print(f"Returning {len(farms_list)} farms with coordinates")
    print("Sample farm data:", farms_list[0] if farms_list else "No farms")
    return jsonify(farms_list)

@app.route('/farmers/count')
def get_farmer_counts():
    df = load_data()
    unique_farmers = len(df.drop_duplicates(subset=['first_name', 'last_name']))
    return jsonify({
        'totalFarmers': unique_farmers,
        'usdaCertifiedCount': 0,  
        'globalGapCertifiedCount': 0  
    })

if __name__ == '__main__':
    app.run(port=8080, debug=True) 
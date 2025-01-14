import os
import csv
import time
from shapely.geometry import Point
from gnews import GNews
from geopy.geocoders import Nominatim
import geopandas as gpd
import pandas as pd
import spacy
from tqdm import tqdm

# Initialize required libraries and services
gnews = GNews(language='en', country='US')
geolocator = Nominatim(user_agent="geoapi")
nlp = spacy.load("en_core_web_sm")

# Load datasets
disaster_data = pd.read_csv("../../events-US-2017-metadata.csv")  # Disaster dataset path
county_data = gpd.read_file("../../../qgis/sheldus_shp/SHELDUS_county_level.shp")  # County shapefile path

# Ensure county geometries are in a projected CRS for accurate centroid calculations
if county_data.crs.is_geographic:
    print("Re-projecting county geometries to a projected CRS...")
    county_data = county_data.to_crs(epsg=3857)  # Web Mercator projection

# Filter disasters with HIGH SHELDUS classification confidence
disaster_data = disaster_data[disaster_data["SHELDUS_CLASSIFICATION_CONFIDENCE"] == "HIGH"]

# Geocode function with caching
geocode_cache = {}

def geocode_location(location):
    if location in geocode_cache:
        return geocode_cache[location]
    try:
        time.sleep(0.5)  # Reduce wait time for faster geocoding
        geo = geolocator.geocode(location)
        if geo:
            geocode_cache[location] = (geo.latitude, geo.longitude)
            return geo.latitude, geo.longitude
        geocode_cache[location] = (None, None)
        return None, None
    except Exception as e:
        print(f"Error geocoding {location}: {e}")
        geocode_cache[location] = (None, None)
        return None, None

# Check if a point lies within a county polygon
def is_within_county(lat, lon, county_polygon):
    point = Point(lon, lat)
    return county_polygon.contains(point)

# Match affected counties based on disaster's EvtName
def get_affected_counties(evt_name, counties_gdf):
    return counties_gdf[counties_gdf["EvtName"] == evt_name]

# Extract and geocode finer-grained locations
def extract_fine_grained_location(article_text, affected_counties):
    doc = nlp(article_text)
    specific_entities = [ent.text for ent in doc.ents if ent.label_ in {"LOC", "FAC", "ORG", "GPE"}]

    for loc in specific_entities:
        lat, lon = geocode_location(loc)
        if lat and lon:
            # Validate the geocoded location against affected counties
            for _, county_row in affected_counties.iterrows():
                if is_within_county(lat, lon, county_row.geometry):
                    return loc, lat, lon  # Return the specific location
    return None, None, None  # Return None if no specific match is found

# Process disasters and collect news articles
for _, disaster in tqdm(disaster_data.iterrows(), total=disaster_data.shape[0], desc="Processing Disasters"):
    # Disaster details
    disaster_name = disaster["Name"]
    search_terms = disaster["Search_Terms_High_Confidence"].replace('"', '').split(", ")
    evt_name = disaster["SHELDUS_Event_Name"]

    # Output file for the disaster
    output_file = f"{disaster_name.replace(' ', '_')}_articles.csv"

    # Skip already processed disasters
    if os.path.exists(output_file):
        print(f"Skipping {disaster_name}, already processed.")
        continue

    # Get affected counties for the disaster
    affected_counties = get_affected_counties(evt_name, county_data)
    if affected_counties.empty:
        print(f"No counties found for disaster: {disaster_name}")
        continue

    # Query and fetch articles for each county
    filtered_articles = []
    for county_name in tqdm(affected_counties["NAME"], desc=f"Fetching Articles for {disaster_name}", leave=False):
        query = f"{' OR '.join(search_terms)} {county_name}"
        articles = gnews.get_news(query)

        for article in articles:
            # Extract article details
            title = article.get("title", "No Title")
            published_date = article.get("published date", "")
            content = article.get("content", "No Content")
            main_image = article.get("image", "No Image")
            url = article.get("url", "No URL")

            # Combine article title and content for location extraction
            text = title + " " + content

            # Try extracting and geocoding finer-grained locations
            location, lat, lon = extract_fine_grained_location(text, affected_counties)

            # Fallback to county name if no finer location is found
            if not location:
                location = county_name
                county_geom = affected_counties[affected_counties["NAME"] == county_name].geometry.iloc[0]
                lat, lon = county_geom.centroid.y, county_geom.centroid.x

            # Append article with geotag details
            filtered_articles.append({
                "title": title,
                "published_date": published_date,
                "content": content,
                "main_image": main_image,
                "location": location,
                "latitude": lat,
                "longitude": lon,
                "url": url,
                "disaster_name": disaster_name,
                "county": county_name
            })

    # Save to CSV
    with open(output_file, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=[
            "title", "published_date", "content", "main_image", "location", 
            "latitude", "longitude", "url", "disaster_name", "county"
        ])
        writer.writeheader()
        writer.writerows(filtered_articles)

    print(f"Articles for {disaster_name} saved to {output_file}")

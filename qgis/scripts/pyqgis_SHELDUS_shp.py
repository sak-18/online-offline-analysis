import os
import pandas as pd
from qgis.core import (
    QgsApplication,
    QgsVectorLayer,
    QgsField,
    QgsFeature,
    QgsVectorFileWriter,
    QgsCoordinateReferenceSystem,
)
from PyQt5.QtCore import QVariant


# Add FIPS field to the temporary layer (copy of the original shapefile)
def add_fips_field(layer):
    if not layer.isEditable():
        layer.startEditing()
    if layer.fields().indexFromName("FIPS") == -1:
        layer.dataProvider().addAttributes([QgsField("FIPS", QVariant.String)])
        layer.updateFields()
        print("FIPS field added.")
    for feature in layer.getFeatures():
        statefp = str(feature["STATEFP"]).zfill(2)
        countyfp = str(feature["COUNTYFP"]).zfill(3)
        layer.changeAttributeValue(feature.id(), layer.fields().indexFromName("FIPS"), f"{statefp}{countyfp}")
    layer.commitChanges()
    print("FIPS field populated.")


# Perform one-to-many join
def one_to_many_join(layer, csv_df, output_shp, output_gpkg):
    print("\nFields in SHP File (before join):")
    print([field.name() for field in layer.fields()])

    print("\nFields in CSV File:")
    print(csv_df.columns.tolist())

    joined_layer = QgsVectorLayer("Polygon?crs=EPSG:4326", "Joined Layer", "memory")
    provider = joined_layer.dataProvider()
    provider.addAttributes(layer.fields())
    for col in csv_df.columns.difference(["FIPS"]):
        provider.addAttributes([QgsField(col, QVariant.String)])
    joined_layer.updateFields()

    print("\nFields in Resulting Layer (after join):")
    print([field.name() for field in joined_layer.fields()])

    csv_dict = csv_df.to_dict(orient="records")
    for feature in layer.getFeatures():
        matching = [row for row in csv_dict if row["FIPS"] == feature["FIPS"]]
        for match in matching:
            new_feature = QgsFeature(joined_layer.fields())
            new_feature.setGeometry(feature.geometry())
            new_feature.setAttributes(feature.attributes() + [match[col] for col in csv_df.columns.difference(["FIPS"])])
            provider.addFeature(new_feature)

    QgsVectorFileWriter.writeAsVectorFormat(joined_layer, output_shp, "UTF-8", QgsCoordinateReferenceSystem("EPSG:4326"), "ESRI Shapefile")
    QgsVectorFileWriter.writeAsVectorFormat(joined_layer, output_gpkg, "UTF-8", QgsCoordinateReferenceSystem("EPSG:4326"), "GPKG")
    print(f"\nShapefile saved: {output_shp}")
    print(f"GeoPackage saved: {output_gpkg}")


# Initialize QGIS
qgis_prefix = "/Applications/MacPorts/QGIS3-LTR.app/Contents/MacOS"
QgsApplication.setPrefixPath(qgis_prefix, True)
qgs = QgsApplication([], False)
qgs.initQgis()

# File paths
shapefile_path = "../usa_shp/tl_2024_us_county/tl_2024_us_county.shp"
csv_path = "../../data/offline/SHELDUS_data/SHELDUS_combined.csv"
output_shapefile = "../sheldus_shp/SHELDUS_county_level.shp"
output_geopackage = "../sheldus_shp/SHELDUS_county_level.gpkg"

# Load original shapefile
original_layer = QgsVectorLayer(shapefile_path, "US Counties", "ogr")
if not original_layer.isValid():
    print("Failed to load shapefile.")
    qgs.exitQgis()
    exit()

# Create a temporary copy of the original shapefile in memory
temporary_layer = QgsVectorLayer(original_layer.dataProvider().dataSourceUri(), "Temporary Layer", "ogr")
if not temporary_layer.isValid():
    print("Failed to create a temporary copy of the shapefile.")
    qgs.exitQgis()
    exit()

# Process data on the temporary layer
add_fips_field(temporary_layer)
csv_data = pd.read_csv(csv_path)
csv_data["FIPS"] = csv_data["FIPS"].astype(str).str.zfill(5)
one_to_many_join(temporary_layer, csv_data, output_shapefile, output_geopackage)

# Exit QGIS
qgs.exitQgis()

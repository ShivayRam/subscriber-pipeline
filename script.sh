#!/bin/bash

# Bash Script to Run Data Cleaning & Promote to prod
# Usage:
#   chmod +x script.sh
#   ./script.sh


echo "Ready to clean the data? [1 for YES/0 for NO]"
read cleancontinue

if [ "$cleancontinue" -ne 1 ]; then
    echo "Cleaning cancelled. Exiting."
    exit 0
fi

echo "Running data cleaning pipeline..."
python src/pipeline.py

#check python output status
if [ $? -ne 0 ]; then
    echo "Pipeline encountered an error. Please fix issues before continuing."
    exit 1
fi

echo "Data cleaning completed."

#read first line of changelog files
DEV_VERSION_LINE=$(head -n 1 changelog.md 2>/dev/null)
PROD_VERSION_LINE=$(head -n 1 prod/changelog.md 2>/dev/null)


# extract version numbers
DEV_VERSION=$(echo $DEV_VERSION_LINE | awk '{print $2}')
PROD_VERSION=$(echo $PROD_VERSION_LINE | awk '{print $2}')

echo "DEV version detected: $DEV_VERSION"
echo "PROD version detected: $PROD_VERSION"


#default PROD_VERSION to empty if none present
if [ -z "$PROD_VERSION" ]; then
    PROD_VERSION="none"
fi

if [ "$DEV_VERSION" != "$PROD_VERSION" ]; then
    echo "New version available. Move cleansed outputs to prod? (1 for YES / 0 for NO)"
    read scriptcontinue
else
    echo "No new version found. Nothing to promote."
    scriptcontinue=0
fi

if [ "$scriptcontinue" -eq 1 ]; then
    echo "Copying updated cleansed artifacts to prod..."

    mkdir -p prod

    cp changelog.md prod/
    cp "prod/cademycode_cleansed.db" prod/
    cp "prod/cademycode_cleansed.csv" prod/

    echo "Files copied to prod successfully."
else
    echo "Promotion cancelled."
fi

echo "Script complete."
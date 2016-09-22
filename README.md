# os-data-fetcher
A script for fetching and pre-processing Ordnance Survey data and preparing them for use

Simply paste the email sent with links from OS into the console and the script will download and prepare the data.

This work was kindly sponsored by Gigaclear plc. - providers of ultrafast fibre broadband.

## Supported datasets

* 1 in 250 000 Scale Colour Raster (raster)
* OS VectorMap District (raster)
* OS Open Map - Local (shape)

## What it does

* Downloads all links in the email
* Merges raster datasets into a single tif with overviews (with compression)
* Merges shapefile datasets and loads into PostGIS

## Using it

First extract the georef data (extract the georef folder to the same place the .zip file is sitting).

Fire-up an OSGeo4W shell and:

`python os_data_fetcher.py --dst_folder C:\tmp\get-os-stuff --dbname db --host host --port 5432 --user user`

The database-related options can be filled with dummy info if you're only working on raster data.

`C:\tmp\get-os-stuff` is a folder where data will be downloaded and processed. Raster results will be seen under `C:\tmp\get-os-stuff\data` 

## Troubleshooting

There's a known issue with running the script with Python 2.7.4 (see [here](http://bugs.python.org/issue17656)).

If you see errors like *TypeError: character mapping must return integer, None or unicode*, please switch to Python 2.7.5 or above.  
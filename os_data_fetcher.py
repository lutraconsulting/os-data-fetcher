"""
os_data_fetcher - script for fetching Ordnance Survey data and 
preparing it for use.
Copyright (C) 2016  Lutra Consulting

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
"""

import os
import urllib
import zipfile
import shutil
import subprocess
import argparse
import psycopg2


class CommandFailed(Exception):
    pass

class OpenDataFetcher():

    def __init__(self, dest_folder, dbname, host, port, user, password, skip_download=False, dst_schema='os'):

        self.db_details = {}
        self.db_details['dbname'] = dbname
        self.db_details['host'] = host
        self.db_details['port'] = port
        self.db_details['user'] = user
        self.db_details['password'] = password

        self.debug = True
        self.skip_download = skip_download

        self.dest_folder = dest_folder
        self.dst_schema = dst_schema
        self.extract_folder = os.path.join(self.dest_folder, 'extract')
        self.data_folder = os.path.join(self.dest_folder, 'data')
        self.georef_folder = os.path.join(os.path.dirname(__file__), 'georef')

        if not os.path.isdir(self.extract_folder):
            os.makedirs(self.extract_folder)
        if not os.path.isdir(self.data_folder):
            os.makedirs(self.data_folder)

        if os.name == 'nt':
            self.convert_cmd = ['magick', 'convert']
        else:
            self.convert_cmd = ['convert']

        try:
            self.run_command(self.convert_cmd)
        except CommandFailed:
            pass
        except WindowsError:
            print
            print 'Could not find ImageMagick on the system PATH'
            print 'Please install a static build of ImageMagick from http://www.imagemagick.org/script/binary-releases.php'
            print 'If ImageMagick is installed, ensure it is on the system PATH'
            print 'e.g. SET PATH=C:\Program Files\ImageMagick-7.0.2-Q16;%PATH%'
            print
            raise Exception()

        self.dataset_identifiers = {
            'http://download.ordnancesurvey.co.uk/open/RAS250': {
                'name': '1 in 250 000 Scale Colour Raster',
                'type': 'raster',
                'data_dir': 'data',
                'misc_options': ['paletted'],
                'georef_path': '250k-raster-tfw-georeferencing-files',
                'tfw_ext': 'TFW'
            },
            'http://download.ordnancesurvey.co.uk/open/VMDRAS': {
                'name': 'OS VectorMap District',
                'type': 'raster',
                'data_dir': 'data',
                'misc_options': ['paletted'],
                'georef_path': '25k-raster-tfw-georeferencing-files/25krastertfw',
                'tfw_ext': 'TFW'
            },
            'http://download.ordnancesurvey.co.uk/open/OPMPLC': {
                'name': 'OS Open Map - Local',
                'type': 'shape',
                'data_dir': 'data',
                'misc_options': [],
                'table_prefix': 'oml_',
                'tables': [
                    'oml_building',
                    'oml_electricitytransmissionline',
                    'oml_foreshore',
                    'oml_functionalsite',
                    'oml_glasshouse',
                    'oml_importantbuilding',
                    'oml_motorwayjunction',
                    'oml_namedplace',
                    'oml_railwaystation',
                    'oml_railwaytrack',
                    'oml_railwaytunnel',
                    'oml_road',
                    'oml_roadtunnel',
                    'oml_roundabout',
                    'oml_surfacewater_area',
                    'oml_surfacewater_line',
                    'oml_tidalboundary',
                    'oml_tidalwater',
                    'oml_woodland'
                ]
            }
        }

        print 'Please paste the email from OS'

        email_content = ''

        while True:
            email_line = raw_input(r'(\q to finish) > ')
            if email_line == r'\q':
                break
            email_content += ( email_line.strip() + '\n' )

        if self.debug:
            print 'The content was:'
            print email_content

        for line in email_content.split('\n'):
            for word in line.split(' '):
                if word.startswith('http://'):
                    for identifier in self.dataset_identifiers.keys():
                        if word.startswith(identifier):
                            if self.debug:
                                print 'Found link to %s' % self.dataset_identifiers[identifier]['name']
                            download_folder = os.path.join(self.dest_folder, 'downloads', self.dataset_identifiers[identifier]['name'])
                            f_name = word.split('/')[-1].split('?')[0]
                            f_path = os.path.join(download_folder, f_name)
                            if not os.path.isdir(download_folder):
                                os.makedirs(download_folder)
                            if os.path.isfile(f_path) and not self.skip_download:
                                os.unlink(f_path)
                            if not os.path.isfile(f_path):
                                o_open = urllib.URLopener()
                                if self.debug:
                                    print 'Downloading %s' % word
                                try:
                                    o_open.retrieve(word, f_path)
                                except IOError:
                                    print 'Failed to download file - please ensure your links are fresh'
                                    break

                            # Extract
                            z = zipfile.ZipFile(f_path, 'r')
                            unzip_path = os.path.join(self.extract_folder, self.dataset_identifiers[identifier]['name'])
                            if not os.path.isdir(unzip_path):
                                os.makedirs(unzip_path)
                            z.extractall(unzip_path)
                            z.close()

                            # Copy data
                            data_path = os.path.join(self.data_folder, self.dataset_identifiers[identifier]['name'])
                            if not os.path.isdir(data_path):
                                os.makedirs(data_path)
                            for root, dirs, files in os.walk(unzip_path):
                                if root.split(os.sep)[-1] == self.dataset_identifiers[identifier]['data_dir']:
                                    for file in files:
                                        shutil.move(os.path.join(root, file), os.path.join(data_path, file))
                                        # Copy geo-ref info
                                        if self.dataset_identifiers[identifier]['type'] == 'raster':
                                            georef_file = file[:-3] + self.dataset_identifiers[identifier]['tfw_ext']
                                            georef_src = os.path.join(self.georef_folder,
                                                                      self.dataset_identifiers[identifier]['georef_path'],
                                                                      georef_file)
                                            georef_dst = os.path.join(data_path, georef_file)
                                            shutil.copy(georef_src, georef_dst)


        self.process_extracted()



    def process_extracted(self):
        # Loop through the content of the data folder and do any processing required
        for folder in os.listdir(self.data_folder):
            folder_path = os.path.join(self.data_folder, folder)
            if not os.path.isdir(folder_path):
                continue
            details = self.get_dataset_details(folder)
            if details is None:
                continue
            if details['type'] == 'raster':
                self.process_raster(folder_path, details)
            else:
                self.process_shape(folder_path, details)

    def process_shape(self, folder_path, details):
        # First drop the destination table(s) if they exist
        if self.debug:
            print 'Preparing for PostGIS import'
        for table in details['tables']:
            fq_table = '"%s"."%s"' % (self.dst_schema, table)
            self.run_sql("""DROP TABLE IF EXISTS """ + fq_table, {})
        self.run_sql("""CREATE SCHEMA IF NOT EXISTS """ + self.dst_schema, {})
        initialised_tables = []
        for f_name in os.listdir(folder_path):
            if not f_name.lower().endswith('.shp'):
                continue
            f_path = os.path.join(folder_path, f_name)
            table_name = details['table_prefix'] + f_name[3:-4].lower()
            if not table_name in initialised_tables:
                args = ['ogr2ogr']
                initialised_tables.append(table_name)
            else:
                args = ['ogr2ogr', '-append']
            args.extend(['--config',
                         'PG_USE_COPY',
                         'YES',
                         '-f',
                         'PostgreSQL',
                         'PG:dbname=\'%(dbname)s\' host=\'%(host)s\' port=\'%(port)s\' user=\'%(user)s\' password=\'%(password)s\'' % self.db_details,
                         '-lco',
                         'SPATIAL_INDEX=FALSE',
                         '-lco',
                         'LAUNDER=no',  # Preserve case of column names to match OS SLDs
                         '-nln',
                         '%s.%s' % (self.dst_schema,table_name),
                         f_path])
            if self.debug:
                print 'Importing %s' % f_name
            self.run_command(args)
        for table in initialised_tables:
            if self.debug:
                print 'Creating index or %s' % table
            self.run_sql("""CREATE INDEX """ + table + """_wkb_geometry_geom_idx ON """ + self.dst_schema + """.""" + table + """ USING gist (wkb_geometry)""", {})

    def process_raster(self, folder_path, details):
        # Convert to RGB is required
        if 'paletted' in details['misc_options']:
            self.convert_paletted_files_to_rgb(folder_path)
        # Create a VRT
        if self.debug:
            print 'Processing %s' % folder_path
            print 'Creating VRT'
        vrt_file_list_path = folder_path + '.txt'
        vrt_file_list = open(vrt_file_list_path, 'w')
        for entry in os.listdir(folder_path):
            p = os.path.join(folder_path, entry)
            if not os.path.isfile(p):
                continue
            if p.lower().endswith('.tif'):
                vrt_file_list.write('%s\n' % p)
        vrt_file_list.close()
        folder_path + os.sep + '*.tif'
        args = ['gdalbuildvrt', '-input_file_list', vrt_file_list_path, folder_path + '.vrt']
        self.run_command(args)
        # Convert to sensible TIF
        if self.debug:
            print 'Creating TIF'
        args = ['gdalwarp', '-overwrite', '-s_srs', 'EPSG:27700', '-t_srs', 'EPSG:27700', '-co', 'TILED=YES', '-co',
                'COMPRESS=JPEG', '-co', 'JPEG_QUALITY=100', '-co', 'BIGTIFF=YES', '-co',  'PHOTOMETRIC=YCBCR', '-co',
                'INTERLEAVE=PIXEL', folder_path + '.vrt', folder_path + '.tif']
        self.run_command(args)
        # Create overviews
        if self.debug:
            print 'Creating overviews'
        args = ['gdaladdo', '--config', 'COMPRESS_OVERVIEW', 'JPEG', '--config', 'JPEG_QUALITY_OVERVIEW', '100', '--config',
                'PHOTOMETRIC_OVERVIEW', 'YCBCR', '--config', 'INTERLEAVE_OVERVIEW', 'PIXEL', '--config', 'BIGTIFF_OVERVIEW',
                'YES', '-ro', '-r', 'average', folder_path + '.tif', '2', '4', '8', '16', '32', '64', '128']
        self.run_command(args)
        # Clean up
        shutil.rmtree(folder_path)
        os.unlink(folder_path + '.vrt')
        os.unlink(folder_path + '.txt')

    def convert_paletted_files_to_rgb(self, folder_path):
        # convert all .tif files under this folder to RGB
        for entry in os.listdir(folder_path):
            if not entry.lower().endswith('.tif'):
                continue
            file_path = os.path.join(folder_path, entry)
            if self.debug:
                print 'Converting %s to RGB' % file_path
            args = []
            args.extend(self.convert_cmd)
            args.extend([file_path, '-type', 'TrueColor', file_path])
            self.run_command(args)

    def run_command(self, args):
        p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        std_out, std_err = p.communicate()
        if p.returncode != 0:
            raise CommandFailed('Failed to run command:\n\n%s\n\nreturn code was %d\n\nstdout was %s\n\nstderr was %s' %
                            (str(args),p.returncode,std_out, std_err))

    def get_dataset_details(self, name):
        for key, ds in self.dataset_identifiers.iteritems():
            if ds['name'] == name:
                return ds
        return None

    def run_sql(self, query, dict):
        con = psycopg2.connect( database = self.db_details['dbname'],
                                user = self.db_details['user'],
                                password = self.db_details['password'],
                                host = self.db_details['host'],
                                port = self.db_details['port'])
        con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cur = con.cursor()
        cur.execute(query, dict)


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('--skip-download', dest="skip_download", help='skip downloading', action='store_true')
    parser.add_argument('--dst_folder', help='destination folder', required=True)
    parser.add_argument('--dbname', help='database name', required=True)
    parser.add_argument('--host', help='host', required=True)
    parser.add_argument('--port', help='port', required=True)
    parser.add_argument('--user', help='user', required=True)

    args = parser.parse_args()

    skip_download = False
    if args.skip_download:
        skip_download = True

    password = raw_input('password for %s >' % args.user)

    odf = OpenDataFetcher(args.dst_folder, args.dbname, args.host, args.port, args.user, password, skip_download=skip_download)

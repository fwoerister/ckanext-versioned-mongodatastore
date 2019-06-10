#!/bin/bash
set -e

echo "This is travis-build.bash..."

echo "Installing the packages that CKAN requires..."
sudo apt-get update
sudo apt-get install solr-jetty redis-server


echo "Installing CKAN and its Python dependencies..."
git clone https://github.com/ckan/ckan
cd ckan
if [ $CKANVERSION == 'master' ]
then
    echo "CKAN version: master"
else
    CKAN_TAG=$(git tag | grep ^ckan-$CKANVERSION | sort --version-sort | tail -n 1)
    git checkout $CKAN_TAG
    echo "CKAN version: ${CKAN_TAG#ckan-}"
fi
# Unpin CKAN's psycopg2 dependency get an important bugfix
# https://stackoverflow.com/questions/47044854/error-installing-psycopg2-2-6-2
sed -i '/psycopg2/c\psycopg2' requirements.txt
python setup.py develop

echo "start solr"
docker build --rm=false -f bin/solr/Dockerfile -t solr .
docker run -d --name solr solr

pip install -r requirements.txt
pip install -r dev-requirements.txt
cd -

echo "Creating the PostgreSQL user and database..."
sudo -u postgres psql -c "CREATE USER ckan_default WITH PASSWORD 'pass';"
sudo -u postgres psql -c 'CREATE DATABASE ckan_default WITH OWNER ckan_default;'

sudo -u postgres psql -c "CREATE USER query_store WITH PASSWORD 'query_store';"
sudo -u postgres psql -c 'CREATE DATABASE query_store WITH OWNER query_store;'

sudo -u postgres psql -c "CREATE USER test_import_db WITH PASSWORD 'test_import_db';"
sudo -u postgres psql -c 'CREATE DATABASE test_import_db WITH OWNER test_import_db;'

echo "check if solr is available ..."
echo "curl http://localhost:8983/solr/"
curl http://localhost:8983/solr/

echo "Initialising the database..."
cd ckan
paster db init -c test-core.ini
cd -

echo "Installing ckanext-pages and its requirements..."
python setup.py develop
pip install -r requirements.txt

echo "Moving test.ini into a subdir..."
mkdir subdir
mv test.ini subdir

echo "Initialising the querystore..."

paster --plugin=ckanext-mongodatastore querystore create_schema --config=subdir/test.ini

echo "travis-build.bash is done."
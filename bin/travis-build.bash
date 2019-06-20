#!/bin/bash
set -e

echo "This is travis-build.bash..."

echo "start solr"

cd bin

wget archive.apache.org/dist/lucene/solr/6.5.0/solr-6.5.0.tgz

tar -xvf solr-6.5.0.tgz
cd solr-6.5.0/bin

sudo ./install_solr_service.sh ../../solr-6.5.0.tgz


cd -

echo "check if solr is available ..."
echo "curl http://localhost:8983/solr/"
curl http://localhost:8983/solr/

echo "Installing the packages that CKAN requires..."
sudo apt-get update
sudo apt-get install python-dev libpq-dev python-pip python-virtualenv git-core openjdk-8-jdk redis-server

sudo apt-get update
sudo apt-get -y install python-software-properties


echo "Installing CKAN and its Python dependencies..."
git clone https://github.com/ckan/ckan
cd ckan
export latest_ckan_release_branch=`git branch --all | grep remotes/origin/release-v | sort -r | sed 's/remotes\/origin\///g' | head -n 1`
echo "CKAN branch: $latest_ckan_release_branch"
git checkout $latest_ckan_release_branch
python setup.py develop
pip install -r requirements.txt --allow-all-external
pip install -r dev-requirements.txt --allow-all-external
cd -

echo "Creating the PostgreSQL user and database..."
sudo -u postgres psql -c "CREATE USER ckan_default WITH PASSWORD 'pass';"
sudo -u postgres psql -c 'CREATE DATABASE ckan_default WITH OWNER ckan_default;'

sudo -u postgres psql -c "CREATE USER query_store WITH PASSWORD 'query_store';"
sudo -u postgres psql -c 'CREATE DATABASE query_store WITH OWNER query_store;'

sudo -u postgres psql -c "CREATE USER test_import_db WITH PASSWORD 'test_import_db';"
sudo -u postgres psql -c 'CREATE DATABASE test_import_db WITH OWNER test_import_db;'


echo "Initialising the database..."
cd ckan
paster db init -c test.ini
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
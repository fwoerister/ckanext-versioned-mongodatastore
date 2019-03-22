.. You should enable this project on travis-ci.org and coveralls.io to make
   these badges work. The necessary Travis and Coverage config files have been
   generated for you.

.. image:: https://travis-ci.org/fwoerister/ckanext-mongodatastore.svg?branch=master
    :target: https://travis-ci.org/fwoerister/ckanext-mongodatastore
    
.. image:: https://coveralls.io/repos/github/fwoerister/ckanext-mongodatastore/badge.svg?branch=master
   :target: https://coveralls.io/github/fwoerister/ckanext-mongodatastore?branch=master

.. image:: https://pypip.in/version/ckanext-mongodatastore/badge.svg
    :target: https://pypi.python.org/pypi/ckanext-mongodatastore/
    :alt: Latest Version

.. image:: https://pypip.in/py_versions/ckanext-mongodatastore/badge.svg
    :target: https://pypi.python.org/pypi/ckanext-mongodatastore/
    :alt: Supported Python versions

.. image:: https://pypip.in/status/ckanext-mongodatastore/badge.svg
    :target: https://pypi.python.org/pypi/ckanext-mongodatastore/
    :alt: Development Status

.. image:: https://pypip.in/license/ckanext-mongodatastore/badge.svg
    :target: https://pypi.python.org/pypi/ckanext-mongodatastore/
    :alt: License


======================
ckanext-mongodatastore
======================

The MongoDatastore is a datastore implementation for CKAN, that uses MongoDB for storing data records. One aspect of this implementation is, that it follows the `RDA recommendations for Data Citation <https://doi.org/10.15497/RDA00016>`_. This guarantiees citability for every query that is submited to this datastore implementation.

.. image:: images/BigPicture.png

As this extension provides an implementation of the IDatastoreBackend, the DataStore API can be used as before.

------------
Requirements
------------

To run this plugin, a CKAN instance already has to be installed. In addition to the PostgreSQL database a MongoDB
instance is required. To retrieve the PIDs in the UI, the CKAN plugin 'datacitator' has to be installed

------------
Installation
------------

.. Add any additional install steps to the list below.
   For example installing any non-Python dependencies or adding any required
   config settings.

To install ckanext-mongodatastore:

1. Activate your CKAN virtual environment, for example::

     . /usr/lib/ckan/default/bin/activate

2. Install the ckanext-mongodatastore Python package into your virtual environment::

     pip install ckanext-mongodatastore

3. Add ``mongodatastore`` to the ``ckan.plugins`` setting in your CKAN
   config file (by default the config file is located at
   ``/etc/ckan/default/production.ini``).

4. Restart CKAN. For example if you've deployed CKAN with Apache on Ubuntu::

     sudo service apache2 reload


---------------
Config Settings
---------------
For running the MongoDb datastore two settings have to configured in your CKAN's configuration file::

    ckan.datastore.write_url = mongodb://[datastorehost]:27017
    ckan.querystore.url = [URL to your query store database]


------------------------
Development Installation
------------------------

To install ckanext-mongodatastore for development, activate your CKAN virtualenv and
do::

    git clone https://github.com/fwoerister/ckanext-mongodatastore.git
    cd ckanext-mongodatastore
    python setup.py develop
    pip install -r dev-requirements.txt


-----------------
Running the Tests
-----------------

To run the tests, do::

    nosetests --nologcapture --with-pylons=test.ini

To run the tests and produce a coverage report, first make sure you have
coverage installed in your virtualenv (``pip install coverage``) then run::

    nosetests --nologcapture --with-pylons=test.ini --with-coverage --cover-package=ckanext.mongodatastore --cover-inclusive --cover-erase --cover-tests


----------------------------------------
Releasing a New Version of ckanext-mongodatastore
----------------------------------------

ckanext-mongodatastore is availabe on PyPI as https://pypi.python.org/pypi/ckanext-mongodatastore.
To publish a new version to PyPI follow these steps:

1. Update the version number in the ``setup.py`` file.
   See `PEP 440 <http://legacy.python.org/dev/peps/pep-0440/#public-version-identifiers>`_
   for how to choose version numbers.

2. Create a source distribution of the new version::

     python setup.py sdist

3. Upload the source distribution to PyPI::

     python setup.py sdist upload

4. Tag the new release of the project on GitHub with the version number from
   the ``setup.py`` file. For example if the version number in ``setup.py`` is
   0.0.2 then do::

       git tag 0.0.2
       git push --tags

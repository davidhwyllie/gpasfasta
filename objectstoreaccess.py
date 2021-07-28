""" analyses objects in OCI's objectstore and loads them into a findNeighbour4 instance 

Note on this code:
It is using batch based processes to scan object stores and find new samples for loading.
Many optimisations (such as the use of messaging queues) are feasible.

Technical notes on the SDK:
Exceptions: https://oracle-cloud-infrastructure-python-sdk.readthedocs.io/en/latest/exceptions.html
Object Storage: https://oracle-cloud-infrastructure-python-sdk.readthedocs.io/en/latest/api/object_storage/osc/oci.object_storage.ObjectStorageosc.html
Code snippet for ObjectStorage:  osc.get_object() method https://docs.oracle.com/en-us/iaas/tools/python-sdk-examples/2.43.0/objectstorage/get_object.py.html

Note: these calls return 'Response' objects, but these are not the same object returned by the Requests library, despite Requests being used internaly by OCI.
They have different attributes and methods, including a 'data' attribute.

However, this 'data' attribute is itself a 'Response object' which is classed as a   oci.request.Response() object, see 
https://oracle-cloud-infrastructure-python-sdk.readthedocs.io/en/latest/api/request_and_response.html.
This object ** is ** (or is very similar to) a Requests library like 'Response' object and has all the expected properties, including .content

"""
import os
import io
import socket
import pandas as pd
import datetime
import oci
import json
import requests
import urllib.parse
import logging
import argparse
import sentry_sdk
import time
from sqlalchemy import (
    Integer,
    Column,
    MetaData,
    String,
    Identity,
    Index,
    TIMESTAMP,
    create_engine)
from sqlalchemy.orm import (
    sessionmaker,
    scoped_session,
)
from sqlalchemy.ext.declarative import declarative_base
from Bio import SeqIO

# global: definition of database structure
# classes mapping to persistence database inherit from this
db_pc = declarative_base()  # global
metadata = MetaData()

# database structure
class FN4LoadError(Exception):
    """a general purpose error used by the fn4load module."""

    pass


class FN4BatchLoadCheck(db_pc):
    __tablename__ = "fn4batchload"
    bl_int_id = Column(
        Integer,
        Identity(start=1),
        primary_key=True,
        comment="the primary key to the table",
    )
    host_name = Column(
        String(16),
        comment="the host machine on which the fn4 server is operating, e.g. fn4dev",
        index=True,
    )
    findneighbour_server_url = Column(
        String(128),
        comment="the host machine on which the fn4 server is operating, e.g. localhost:5025",
        index=True,
    )
    check_time = Column(
        TIMESTAMP, comment="the time the batch of samples started", index=True
    )
    batch_size = Column(Integer, comment="the number in the batch")
    number_in_server = Column(Integer, comment="the number in the batch")


class FN4LoadAttempt(db_pc):
    """stores the results of the GPAS to FN4 feed for one or more FN4 databases"""

    __tablename__ = "fn4loadattempt"
    gp_int_id = Column(
        Integer,
        Identity(start=1),
        primary_key=True,
        comment="the primary key to the table",
    )
    host_name = Column(
        String(16),
        comment="the host machine on which the fn4 server is operating, e.g. fn4dev",
    )
    findneighbour_server_url = Column(
        String(128),
        comment="the host machine on which the fn4 server is operating, e.g. fn4dev",
    )
    parse_status = Column(String(20), comment="the result of parsing the fasta file")
    batch_start_time = Column(
        TIMESTAMP, comment="the time the batch of samples started"
    )
    batch_sample_number = Column(Integer, comment="the number in the batch")
    batch_size = Column(Integer, comment="the batch size")
    batch_sample_number = Column(Integer, comment="the batch size")
    seqid = Column(String(38), comment="the sequence id", nullable=True)
    file_name = Column(String(128), comment="the sequence id")
    len_seq = Column(Integer, nullable=True)
    first_128_chars = Column(String(128), nullable=True)
    bucket_read_start_time = Column(
        TIMESTAMP, comment="the time the bucket read started", nullable=True
    )
    bucket_read_end_time = Column(
        TIMESTAMP, comment="the time the bucket read end", nullable=True
    )
    fn4id = Column(String(38), nullable=True)
    insert_start_time = Column(
        TIMESTAMP, comment="the time the fn4 insert started", nullable=True
    )
    insert_end_time = Column(
        TIMESTAMP, comment="the time the fn4 insert end", nullable=True
    )
    insert_status_code = Column(
        Integer, comment="http status code from the insert", nullable=True
    )
    completed = Column(Integer)


Index(
    "check_ix",
    FN4LoadAttempt.findneighbour_server_url,
    FN4LoadAttempt.host_name,
    FN4LoadAttempt.completed,
)


class BucketAccess:
    """provides methods to access files in OCI object store buckets"""

    def __init__(
        self,
        namespace_name,
        bucket_name,
        config_file_location="/home/ubuntu/.oci/config",
        profile_name="DEFAULT",
    ):
        """prepare to perform operations on an OCI Objectstore bucket.

        Parameters
        ==========
        namespace_name: the OCI bucket namespace name
        bucket_name: the OCI bucket name
        config_file_location: the OCI configuration file location.  If None, performs Instance principal based authentication
        profile_name: the profile with the configuration file from which to read credentials.  Ignored if config_file_location is None.

        Note: as a prerequisite, you need to set up an OCI credentials file in the location 'config_file_location';
        see https://oracle-cloud-infrastructure-python-sdk.readthedocs.io/en/latest/configuration.html and also README.md
        """

        self.namespace_name = (namespace_name,)
        self.bucket_name = (bucket_name,)
        self.config_file_location = config_file_location
        self.profile_name = profile_name

        if config_file_location is not None:
            # make a connection config from the file provided 
            self.config = oci.config.from_file(
                profile_name=self.profile_name, file_location=self.config_file_location
            )
            self.signer = None
            oci.config.validate_config(self.config)

            # make ObjectStorageClient
            self.osc = oci.object_storage.ObjectStorageClient(self.config)

        else:
            # perform instance principal based authentication
            # create the signer

            try:
                # get signer from instance principals token
                self.signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
            except Exception:
                print("There was an error while trying to get the Signer")
                raise SystemExit

            # generate config info from signer
            self.config = {'region': self.signer.region, 'tenancy': self.signer.tenancy_id}

            # initiate the object client

            self.osc = oci.object_storage.ObjectStorageClient(config = {}, signer=self.signer)


    def list_files(self):
        """lists files/objects in the bucket

        Parameters:
        None

        Returns:
        Pandas data frame containing filenames, sorted by date, in ascending order, if such files exist;
        or None if they do not

        Raises:
        Any errors encountered during the process
        """

        ## TODO: error trapping
        all_files = oci.pagination.list_call_get_all_results(
            self.osc.list_objects,
            namespace_name=self.namespace_name,
            bucket_name=self.bucket_name,
            fields="name,etag,size,timeCreated,md5,storageTier,archivalState",
        )

        retdict = dict()
        for item in all_files.data.objects:
            retdict[item.etag] = dict(
                time_created=item.time_created,
                md5=item.md5,
                name=item.name,
                size=item.size,
            )

        pending_files = pd.DataFrame.from_dict(retdict, orient="index")

        if "time_created" in pending_files.columns:
            return pending_files.sort_values(by="time_created", ascending=True)
        else:
            return None

    def load_bucket_into_string(self, object_name):
        """loads the contents of a file into a string

        Parameters:
        object_name:  the name of the object to be downloaded

        Returns: a string contining the file's contents

        Raises:
        Any errors encountered during the process.
        If the object does not exists, an oci.exceptions.ServiceError is raised with .code property = 'ObjectNotFound'
        """

        res = self.osc.get_object(
            namespace_name=self.namespace_name,
            bucket_name=self.bucket_name,
            object_name=object_name,
        )
        return res.data.content.decode("utf-8")

    def save_string_into_object(self, object_name, object_content):
        """saves the contents of a string into an object

        Parameters:
        object_name:  the name of the object to be written to
        object_content: a string containing the file content

        Returns:
        "success" if added or "already exists" if it already exists

        Raises:
        Any errors encountered, except for errors indicating that the file already exists, which are trapped

        """

        if not isinstance(object_content, str):
            raise TypeError(
                "Object_content must be a string, but it is a {0}".format(
                    type(object_content)
                )
            )

        # Will not overwrite existing data. Will fail silently if asked to do so.
        result = "success"

        try:
            self.osc.put_object(
                namespace_name=self.namespace_name,
                bucket_name=self.bucket_name,
                object_name=object_name,
                put_object_body=object_content.encode("utf-8"),
                if_none_match="*",
            )
        except oci.exceptions.ServiceError as e:
            if e.code == "IfNoneMatchFailed":
                result = "already exists"  # file already exists
            else:
                sentry_sdk.capture(e)
                raise
        return result

    def delete_object(self, object_name):
        """deletes an object

        Parameters:
        object_name:  the name of the object to be written to

        Returns:
        None

        Raises:
        Any errors encountered

        """

        self.osc.delete_object(
            namespace_name=self.namespace_name,
            bucket_name=self.bucket_name,
            object_name=object_name,
        )

        return None


class ObjectStore2FN4:
    """Links GPAS objectstore fasta outputs and findNeighbour4; part of the GPAS system

    provides methods to
    - load fasta files and write them to a findNeighbour4 instance;
    - record the results of the attempts to write to findNeighbour4 in a database
    """

    def __init__(
        self,
        findneighbour_server_url,
        namespace_name,
        input_bucket_name,
        connection_config=None,
        debug=0,
    ):
        """creates the objectstore2fn4 object.

        Example usage:
        os2fn4 = ObjectStore2FN4(
            findneighbour_server_url = 'http://localhost:5025',
            namespace_name = 'lxxxxxxxxxx',
            input_bucket_name = 'FN4-queue',
            connection_config='prod'
        )

        Parameters:
        findneighbour_server_url: the url of the targeted findneighbour server
        namespace_name: the namespace_name of the OCI object store
        input_bucket_name: name of the bucket to read from
        connection_config: reporting database configuration.  See below; if not supplied, logging occurs to an (ephemeral) in memory sqlite database)
        debug: if set to 2, deletes data in the database, a setting useful for unit testing.

        connection_config:
        One of
        1. a key to a dictionary containing one or more database configuration details: (e.g. 'prod', 'test')
        2. a valid sqlalchemy database connection string (if this is sufficient for connections)  e.g. 'pyodbc+mssql://myserver'
        3. None.  This is considered to mean 'sqlite://' i.e. an in memory sqlite database, which is not persisted when the program stops.

        if it is not none, a variable called DB_CONNECTION_CONFIG_FILE must be present.  This must point to a file containing credentials.
        the name of an environment variable containing (in json format) a dictionary, or None if it is not required.
        An example of such a dictionary is as below:
        {
            'prod':{'DBTYPE':'sqlite', 'ENGINE_NAME':'sqlite:///db/proddb.sqlite'}
            'dev': {'DBTYPE':'sqlite', 'ENGINE_NAME':'sqlite:///db/devdb.sqlite'},
            'test':{'DBTYPE':'sqlite', 'ENGINE_NAME':'sqlite://'}
        }
        The DBTYPE and ENGINE_NAME keys are essential.
        Other keys may also be present, and are required in some cases (for example by Oracle connections).
        {
            'prod':{'DBTYPE':'oracle',
                    'ENGINE_NAME':''oracle+cx_oracle://PROD:97bxxxxxxxxX@(description: .........)))',
                    'TNS_ADMIN':'/secrets/oracle/pca_prod'
                    },

            'dev':{'DBTYPE':'oracle',
                    'ENGINE_NAME':''oracle+cx_oracle://PROD:97bxxxxxxxxX@(description: .........)))',
                    'TNS_ADMIN':'/secrets/oracle/pca_prod'
                    }
        }
        Note, the bit after the @(description describes where your database is, and will be found in your cloud wallet, if you are using cloud databases.  See below.
        In this case, TNS_ADMIN is the value you wish the TNS_ADMIN environment variable to be set to.
        The software will set TNS_ADMIN, and, if you are using a virtual environment, it will be scoped to the virtual environment.
        In summary, it will configure necessary settings to allow Oracle database connections.

        Note, if you are using a python virtual environment, this environment variable should be included in the .env file in the root of your project.  The .env file should not be under source control.

        configuration engine_name: an SQLalchemy connect string, e.g. sqlite::// for temporary memory db, see https://docs.sqlalchemy.org/en/13/core/engines.html
        debug: if True, deletes any existing data on startup.
        show_bar: show a progress bar during long operations


        NOTE:
        This software has been tested with
        (i) Sqlite 3.3.2+
        (ii) Oracle Autonomous Database (cloud)
        https://blogs.oracle.com/oraclemagazine/getting-started-with-autonomous

        To connect to Oracle there are several steps.
        0. Install dependencies, see
        https://cx-oracle.readthedocs.io/en/latest/user_guide/installation.html
        wget https://download.oracle.com/otn_software/linux/instantclient/211000/instantclient-basic-linux.x64-21.1.0.0.0.zip
        need to set the LD_LIBRARY_PATH variable, see
        https://www.oracle.com/database/technologies/instant-client/linux-x86-64-downloads.html
        e.g. export LD_LIBRARY_PATH=/data/software/instantclient_21_1:LD_LIBRARY_PATH

        these parameters have to go into the python .env file e.g.
        ----------------------------------------------------------------
        LD_LIBRARY_PATH="/software/instantclient_21_1"
        PCADB_CONNECTION_CONFIGFILE="/secret/config.json"

        Where config.json looks like
        {
            'prod':{'DBTYPE':'oracle',
                    'ENGINE_NAME':''oracle+cx_oracle://PROD:97bxxxxxxxxX@(description: .........)))',
                    'TNS_ADMIN':'/secrets/oracle/pca_prod'
                    }
        }
        ** NOTE: as per normal json conventions, escape quotes (i.e. \" not " around the certificate name, otherwise SSL connections will fail)  **

        1. Download your OCI wallet, & unzip it somewhere
        2. Set the TNS_ADMIN env var to point to this directory
        3. Edit the WALLET_LOCATION in the sqlnet.ora file to point to the relevant directory, e.g. WALLET_LOCATION = (SOURCE = (METHOD = file) (METHOD_DATA = (DIRECTORY="/data/credentials/oci_test")))
        4. Create a user with relevant privileges see below)
        5. Set the OCI_ENGINE_NAME env var.
        An example of this is as below (redacted so not live)
        oracle+cx_oracle://scott:tigerX22@(description= (retry_count=20)(retry_delay=3)(address=(protocol=tcps)(port=1522)(host=host.oraclecloud.com))(connect_data=(service_name=redacted))(security=(ssl_server_cert_dn="redacted")))


        This data can be found in the tnsnames.ora file: for details, see
         https://docs.sqlalchemy.org/en/14/dialects/oracle.html#dialect-oracle-cx_oracle-connect
         https://stackoverflow.com/questions/37471892/using-sqlalchemy-dburi-with-oracle-using-external-password-store
         https://stackoverflow.com/questions/14140902/using-oracle-service-names-with-sqlalchemy/35215324
         https://blogs.oracle.com/sql/how-to-create-users-grant-them-privileges-and-remove-them-in-oracle-database
         https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/GRANT.html#GUID-20B4E2C0-A7F8-4BC8-A5E8-BE61BDC41AC3

        Configuring interactions with external OCI databases
        ====================================================
        Your application will need to run as a user (we'll call it PCADB) will need some priviledges granted.
        The exact privileges required involving creating, dropping tables & indexes, as well as inserting and deleting data.
        CREATE USER PCADB IDENTIFIED BY 'MyPassword1234!';
        GRANT CONNECT TO PCADB;
        GRANT CREATE SESSION TO PCADB;
        GRANT CREATE SEQUENCE TO PCADB;
        GRANT CREATE TABLE TO PCADB;
        GRANT CREATE SYNONYM TO PCADB;
        ALTER USER PCADB DEFAULT TABLESPACE DATA quota unlimited on DATA;

        """

        self.findneighbour_server_url = findneighbour_server_url
        self.namespace_name = namespace_name
        self.input_bucket_name = input_bucket_name
        self.host_name = socket.gethostname()

        # note the below code is from findNeighbour4.rdbmsstore.
        # connect and create session.  Validate inputs carefully.
        if connection_config is None:
            logging.info("Connection config is None: using in-memory sqlite.")
            self.engine_name = "sqlite://"

        elif "://" in connection_config:
            # it's not None, and we assume what we are provided is an sqlalchemy database connection string
            logging.info(
                "Connection config provided; using {0}".format(connection_config)
            )
            self.engine_name = connection_config
        else:
            # we have been passed a token.  this should be a key to a dictionary, stored in
            # DB_CONNECTION_CONFIG_FILE which contains credentials
            conn_detail_file = None
            try:
                conn_detail_file = os.environ["DB_CONNECTION_CONFIG_FILE"]
            except KeyError:
                raise FN4LoadError(
                    "Environment variable DB_CONNECTION_CONFIG_FILE does not exist; however, it is required.  If you are using a python virtual environment, you need to set it in .env, not globally"
                )

            if conn_detail_file is None:
                # we failed to set it
                raise FN4LoadError(
                    "Tried to set conn_detail_file from environment variable DB_CONNECTION_CONFIG_FILE, but it is still None."
                )

            if not os.path.exists(conn_detail_file):
                raise FileNotFoundError(
                    "Connection file specified but not found: {0}".format(
                        conn_detail_file
                    )
                )

            # read the config file
            with open(conn_detail_file, "rt") as f:
                conn_detail = json.load(f)

            if connection_config not in conn_detail.keys():
                raise FN4LoadError(
                    "Connection {0} does not correspond to one of the keys {1} of the configuration json file at {2}".format(
                        connection_config, conn_detail.keys(), conn_detail_file
                    )
                )

            # configure engine
            this_configuration = conn_detail[
                connection_config
            ]  # extract the relevant part of the config dictionary

            # two keys are always present
            essential_keys = set(["DBTYPE", "ENGINE_NAME"])
            if len(essential_keys - set(this_configuration.keys())) > 0:
                raise FN4LoadError(
                    "Provided keys for {0} are not correct.  Required are {1}".format(
                        connection_config, essential_keys
                    )
                )

            # if it's Oracle, then three keys are required.
            if this_configuration["DBTYPE"] == "oracle":
                essential_keys = set(["DBTYPE", "ENGINE_NAME", "TNS_ADMIN"])
                if len(essential_keys - set(this_configuration.keys())) > 0:
                    raise FN4LoadError(
                        "Provided keys for oracle db in {0} are not correct.  Required are {1}".format(
                            connection_config, essential_keys
                        )
                    )

                # set the TNS_ADMIN variable.
                logging.info(
                    "Set TNS_ADMIN to value specified in config file {0}".format(
                        this_configuration["TNS_ADMIN"]
                    )
                )
                os.environ["TNS_ADMIN"] = this_configuration["TNS_ADMIN"]

            logging.info("Set ENGINE_NAME configuration string from config file.")
            self.engine_name = this_configuration["ENGINE_NAME"]

        self.using_sqlite = self.engine_name.startswith("sqlite://")

        # now we can start
        self.Base = db_pc
        logging.info("DatabaseManager: Connecting to database")
        self.engine = create_engine(self.engine_name)
        self.is_oracle = "oracle+cx" in self.engine_name
        self.is_sqlite = "sqlite://" in self.engine_name

        self.Base.metadata.create_all(
            bind=self.engine
        )  # create the table(s) if they don't exist

        # create thread-local sessions, see https://docs.sqlalchemy.org/en/14/orm/contextual.html#using-custom-created-scopes
        session_factory = sessionmaker(bind=self.engine)

        # this object will call session factory when we create/request a thread local session
        # e.g. thread_local_session = self.Session()
        self.Session = scoped_session(session_factory)

        # drop existing tables if in debug mode
        # delete any pre-existing data if we are in debug mode.
        if debug == 2:
            logging.warning(
                "Debug mode operational [DEBUG={0}]; deleting all data from tables.".format(
                    debug
                )
            )
            self._delete_existing_data()

        else:
            logging.info("Using stored data in rdbms")

    def _absurl(self, relpath):
        """constructs an absolute URL from the relative path requested"""

        return urllib.parse.urljoin(self.findneighbour_server_url, relpath)

    def _getpost(self, relpath, method="GET", payload=None, timeout=None):
        """issues GET or POST against url.  returns a response object
        will raise errors if generated, but does not raise errors if a valid
        response object is returned, even if it has a status_code indicating the call failed
        """

        session = requests.Session()
        session.trust_env = False

        url = self._absurl(relpath)
        if method == "GET":
            response = session.get(url=url)

        elif method == "POST":
            response = session.post(url=url, data=payload)

        else:
            raise NotImplementedError(
                "either GET or POST is required as a method.  was passed {0}".format(
                    method
                )
            )

        session.close()

        if response.status_code >= 500:
            response.raise_for_status()  # raise error if there's an error.  Sub 500 (404 etc) are let through and handled by the client routines
        return response

    def filename2fn4id(self, x):
        """removes the "GPAS|" prefix from fn4id and anything after a .
        Parameters:
        x: a filename

        Returns:
        either None, if there's no GPAS tag, or x stripped of the GPAS| prefix"""
        if x.startswith("GPAS_SP3"):
            return x.replace("GPAS_SP3|", "")
        else:
            return None

    def _decode(self, response):
        """checks that a response object has response code in the 200s.
        If it doesn't, raises an error.
        If it does, decodes the object and returns json."""
        response.raise_for_status()
        return json.loads(response.content.decode("utf-8"))

    def _get(self, relpath, timeout=None):
        """issues GET against url.  returns a response object.
        raises errors if fails"""
        response = self._getpost(relpath=relpath, timeout=timeout, method="GET")
        return response

    def _post(self, relpath, payload, timeout=None):
        """issues  POST against url.  returns a response object."""
        response = self._getpost(
            relpath=relpath, payload=payload, timeout=timeout, method="POST"
        )
        return response

    def _insert(self, guid, seq, timeout=None):
        """inserts a sequence seq with guid"""

        # check input
        if not isinstance(guid, str):
            raise TypeError(
                "guid {0} passed must be a string, not a {1}".format(guid, type(guid))
            )
        if not isinstance(seq, str):
            raise TypeError(
                "sequence passed must be a string, not a {0}".format(type(seq))
            )
        return self._post(
            "/api/v2/insert", payload={"guid": guid, "seq": seq}, timeout=timeout
        )

    def _delete_existing_data(self) -> None:
        """deletes existing data from the databases"""

        tls = (
            self.Session()
        )  # thread local session ; will be reused if need be transparently
        tls.query(FN4BatchLoadCheck).delete()
        tls.query(FN4LoadAttempt).delete()
        tls.commit()

    def guids(self, timeout=None):
        """returns all guids in the server"""
        return self._decode(self._getpost("/api/v2/guids", method="GET"))

    def insert_files_into_server(self):
        """obtain the fasta files, load them into the findneighbour server"""

        tls = (
            self.Session()
        )  # thread local session ; will be reused if need be transparently

        input_ba = BucketAccess(
            namespace_name=self.namespace_name, bucket_name=self.input_bucket_name
        )

        current_files = input_ba.list_files()
        if current_files is None:
            return {"added": 0}  # nothing  to insert

        guids = self.guids()
        logging.info("Recovered {0} sample_ids from server".format(len(guids)))
        logging.info(
            "Recovered {0} samples from object store".format(len(current_files))
        )

        fn4ids = list()
        for filename in current_files["name"]:
            fn4id = filename.split(".")[0]  # the bit before the .
            fn4ids.append(fn4id)
        current_files["fn4id"] = fn4ids
        fn4ids = set(fn4ids)
        to_add = fn4ids - set(guids)

        logging.info(
            "There are {0} samples to add; checking these have not already failed".format(
                len(to_add)
            )
        )

        tls = (
            self.Session()
        )  # thread local session ; will be reused if need be transparently
        completed = list()
        logging.info(
            "There are {0} completed samples identified".format(len(completed))
        )
        for (fn4id,) in (
            tls.query(FN4LoadAttempt.fn4id)
            .filter(FN4LoadAttempt.host_name == self.host_name)
            .filter(
                FN4LoadAttempt.findneighbour_server_url == self.findneighbour_server_url
            )
            .filter(completed == 1)
        ):
            completed.append(fn4id)
        completed = set(completed)

        to_add = to_add - completed
        logging.info("There are {0} samples to add".format(len(to_add)))

        current_files = current_files[
            current_files["fn4id"].isin(to_add)
        ]  # select what we need to add

        batch_start_time = datetime.datetime.now()

        bc = FN4BatchLoadCheck(
            host_name=self.host_name,
            findneighbour_server_url=self.findneighbour_server_url,
            check_time=datetime.datetime.now(),
            batch_size=len(current_files.index),
            number_in_server=len(guids),
        )
        tls.add(bc)
        tls.commit()

        for i, file_name in enumerate(current_files["name"]):

            bucket_read_start_time = datetime.datetime.now()
            file_content = input_ba.load_bucket_into_string(object_name=file_name)
            bucket_read_end_time = datetime.datetime.now()

            # parse the file using biopython
            nFiles = 0
            res = {
                "host_name": self.host_name,
                "findneighbour_server_url": self.findneighbour_server_url,
                "parse_status": "Failed",
                "batch_start_time": batch_start_time,
                "batch_sample_number": i,
                "batch_size": len(to_add),
                "seqid": None,
                "file_name": file_name,
                "len_seq": None,
                "first_128_chars": file_content[:128],
                "bucket_read_start_time": bucket_read_start_time,
                "bucket_read_end_time": bucket_read_end_time,
                "fn4id": None,
                "insert_start_time": None,
                "insert_end_time": None,
                "insert_status_code": None,
            }

            seq = None

            with io.StringIO(file_content) as f:

                for record in SeqIO.parse(f, "fasta"):
                    nFiles += 1
                    if nFiles > 1:  # that's a multifasta, and we don't support that
                        res["parse_status"] = "Failed, multifasta"

                    else:
                        res["parse_status"] = "Success"
                        res["seqid"] = str(record.id)
                        res["file_name"] = file_name
                        res["len_seq"] = len(str(record.seq))
                        seq = str(record.seq)

            if res["parse_status"] == "Success":
                res["fn4id"] = self.filename2fn4id(res["seqid"])

                if res["fn4id"] is None:
                    res["parse_status"] = "Failed, seqid does not start with GPAS"

            if res["fn4id"] is not None:

                res["insert_start_time"] = datetime.datetime.now()
                insert_response = self._insert(res["fn4id"], seq)
                res["insert_end_time"] = datetime.datetime.now()
                res["insert_status_code"] = insert_response.status_code

            # decide whether to move the input file to a new folder (this is currently disabled).
            # We do this unless the findNeighbour server errored.
            move_file = 0
            if not res["parse_status"] == "Success":
                move_file = 1
            if res["insert_status_code"] == 200:
                move_file = 1
            res["completed"] = move_file
            logging.info(
                "Adding {0} {1} {2} {3}".format(
                    datetime.datetime.now().isoformat(), i, res["fn4id"], move_file
                )
            )

            tls.add(FN4LoadAttempt(**res))
            tls.commit()
        return {"added": i}
        print("Finished")


if __name__ == "__main__":

    # command line usage.
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawTextHelpFormatter,
        description="""Loads GPAS fasta file output from an object store into a findNeighbour4 instance.

                    Example usage:

                        pipenv run python3 objectstoreaccess.py "http://localhost:5025" "lrbvkel2wjot" "FN4-queue" "unittest_oracle"

                        Note that once successfully launched, 
                        the loader will run forever, 
                        and will never terminate unless killed.
                                """,
    )
    parser.add_argument(
        "findneighbour_server_url",
        type=str,
        action="store",
        nargs="?",
        help="the findneighbour_server_url",
    )

    parser.add_argument(
        "namespace_name",
        help="the oci bucket namespace_name from which to read.",
        type=str,
        action="store",
        nargs="?",
    )

    parser.add_argument(
        "input_bucket_name",
        help="the oci bucket_name from which to read.",
        type=str,
        action="store",
        nargs="?",
    )

    parser.add_argument(
        "connection_config",
        help="the database access string, or the key to an oracle connection dictionary.  see docs",
        type=str,
        action="store",
        nargs="?",
    )
    parser.add_argument(
        "--debug",
        help="default = 0 (normal operations).  If set to 2, will delete any existing data in the database before resuming.",
        type=int,
        action="store",
        nargs="?",
        default=0,
    )

    args = parser.parse_args()

    if os.environ.get("FN_SENTRY_URL") is not None:
        logging.info("Launching communication with Sentry bug-tracking service")
        sentry_sdk.init(os.environ.get("FN_SENTRY_URL"))

    logging.info(
        "Watching object store and loading of this into findneighbour4.  Progress is logged to database.  No more log entries will be written, unless the software errors."
    )
    os2fn4 = ObjectStore2FN4(
        findneighbour_server_url=args.findneighbour_server_url,
        namespace_name=args.namespace_name,
        input_bucket_name=args.input_bucket_name,
        connection_config=args.connection_config,
        debug=args.debug
    )

    while True:

        n_inserted = os2fn4.insert_files_into_server()

        if n_inserted == 0:
            time.sleep(180)     # seconds

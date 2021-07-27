# unit tests
import os
import oci
import unittest
import pandas as pd
from objectstoreaccess import BucketAccess

# need to be configure to the particular tenancey in which you are working
# also, it is assumed that a completed credentials file is present in the default location, see below.


class BucketAccess_startup(unittest.TestCase):
    def setUp(self):
        """sets namespace and bucket_name from the environment variables
        NAMESPACE_NAME and
        BUCKET_NAME
        which should be declared in the .env file (if using a virtual environment) or globally if not.
        """

        try:
            self.NAMESPACE_NAME = os.environ.get("NAMESPACE_NAME")
        except KeyError:
            self.fail(
                "You must create a NAMESPACE_NAME environment variable including the namespace of the OCI bucket.  If using a virtual environment, this should be in the .env file"
            )

        try:
            self.BUCKET_NAME = os.environ.get("BUCKET_NAME")
        except KeyError:
            self.fail(
                "You must create a BUCKET_NAME environment variable including the name of the OCI bucket.  If using a virtual environment, this should be in the .env file"
            )


class Test_BucketAccess_1(BucketAccess_startup):
    """tests the BucketAccess class."""

    def runTest(self):

        ba = BucketAccess(
            namespace_name=self.NAMESPACE_NAME, bucket_name=self.BUCKET_NAME
        )

        # there should be nothing in the bucket to start with, but we cannot guarantee this
        current_files = ba.list_files()
        if current_files is not None:  # there are some
            for filename in current_files["name"]:
                ba.delete_object(object_name=filename)  # delete them
                print(filename)

        # check that where there are no files, list_files returns None.
        current_files = ba.list_files()
        self.assertIsNone(current_files)

        # check that if we ask to recover an object which does not exist, an error is raised.
        with self.assertRaises(oci.exceptions.ServiceError):
            ba.load_bucket_into_string(object_name="doesnotexist.txt")

        # write a file to the object store
        object_content = "david was here  putting this into Object storage"
        ba.save_string_into_object(
            object_name="unittesting_wrote_this.txt", object_content=object_content
        )

        # check this is listed
        current_files = ba.list_files()
        self.assertIsInstance(current_files, pd.DataFrame)
        self.assertEqual(len(current_files.index), 1)

        # check we can read the string back
        res = ba.load_bucket_into_string(object_name="unittesting_wrote_this.txt")
        self.assertEqual(res, object_content)

        # delete the object
        ba.delete_object(object_name="unittesting_wrote_this.txt")

        # check it has gone
        current_files = ba.list_files()
        self.assertIsNone(current_files)


class Test_BucketAccess_2(BucketAccess_startup):
    """tests the BucketAccess class."""

    def runTest(self):

        ba = BucketAccess(
            namespace_name=self.NAMESPACE_NAME, bucket_name=self.BUCKET_NAME
        )

        # there should be nothing in the bucket to start with, but we cannot guarantee this
        current_files = ba.list_files()
        if current_files is not None:  # there are some
            for filename in current_files["name"]:
                ba.delete_object(object_name=filename)  # delete them

        object_content = "unittests put this into Object storage"

        # add 50 objects - check that we recover them all.
        # To test pagination fully (which is a core Oracle class, so should work, need to insert > 1000
        for i in range(50):
            object_name = "test_object_{0}.txt".format(i)
            ba.save_string_into_object(
                object_name=object_name, object_content=object_content
            )

        # check these are all listed
        current_files = ba.list_files()
        self.assertIsInstance(current_files, pd.DataFrame)
        self.assertEqual(len(current_files.index), 50)

        # remove everything
        current_files = ba.list_files()
        if current_files is not None:  # there are some
            for filename in current_files["name"]:
                ba.delete_object(object_name=filename)  # delete them

        # check these are all listed
        current_files = ba.list_files()
        self.assertIsNone(current_files)

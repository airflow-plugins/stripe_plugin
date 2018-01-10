import logging
import json
import collections
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from stripe_plugin.hooks.stripe_hook import StripeHook
from tempfile import NamedTemporaryFile


class StripeToS3Operator(BaseOperator):
    """
    Make a query against Stripe and write the resulting data to s3
    """
    template_field = ('s3_key', )

    @apply_defaults
    def __init__(self,
                 stripe_conn_id,
                 stripe_object,
                 stripe_args={},
                 s3_conn_id=None,
                 s3_key=None,
                 s3_bucket=None,
                 fields=None,
                 replication_key_value=0,
                 *args,
                 **kwargs
                 ):
        """ 
        Initialize the operator
        :param stripe_conn_id:          name of the Airflow connection that has
                                        your Stripe username, password and user_key
        :param stripe_object:            name of the Stripe object we are
                                        fetching data from
        :param stripe_args              *(optional)* dictionary with extra stripe
                                        arguments
        :param s3_conn_id:              name of the Airflow connection that has
                                        your Amazon S3 conection params
        :param s3_bucket:               name of the destination S3 bucketcd
        :param s3_key:                  name of the destination file from bucket
        :param fields:                  *(optional)* list of fields that you want
                                        to get from the object.
                                        If *None*, then this will get all fields
                                        for the object
        :param replication_key_value:   *(optional)* value of the replication key,
                                        if needed. The operator will import only 
                                        results with the id grater than the value of
                                        this param.
        """

        super().__init__(*args, **kwargs)

        self.stripe_conn_id = stripe_conn_id
        self.stripe_object = stripe_object
        self.stripe_args = stripe_args

        self.s3_conn_id = s3_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key

        self.fields = fields
        self.replication_key_value = replication_key_value
        self._kwargs = kwargs

    def filter_fields(self, result):
        """
        Filter the fields from an resulting object.

        This will return a object only with fields given
        as parameter in the constructor.

        All fields are returned when "fields" param is None.
        """
        if not self.fields:
            return result
        obj = {}
        for field in self.fields:
            obj[field] = result[field]
        return obj

    def execute(self, context):
        """
        Execute the operator.
        This will get all the data for a particular Stripe model
        and write it to a file.
        """
        logging.info("Prepping to gather data from Stripe")
        hook = StripeHook(
            conn_id=self.stripe_conn_id
        )

        # attempt to connect to Stripe
        # if this process fails, it will raise an error and die right here
        # we could wrap it
        hook.get_conn()

        logging.info(
            "Making request for"
            " {0} object".format(self.stripe_object)
        )

        results = hook.run_query(
            self.stripe_object,
            self.replication_key_value,
            **self.stripe_args)


        # write the results to a temporary file and save that file to s3
        with NamedTemporaryFile("w") as tmp:
            for result in results:
                filtered_result = self.filter_fields(result)
                tmp.write(json.dumps(filtered_result) + '\n')

            tmp.flush()

            dest_s3 = S3Hook(s3_conn_id=self.s3_conn_id)
            dest_s3.load_file(
                filename=tmp.name,
                key=self.s3_key,
                bucket_name=self.s3_bucket,
                replace=True

            )
            dest_s3.connection.close()
            tmp.close()

    logging.info("Query finished!")

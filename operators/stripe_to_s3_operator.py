from tempfile import NamedTemporaryFile
import logging
import json
import os

from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator, SkipMixin
from airflow.utils.decorators import apply_defaults

from stripe_plugin.hooks.stripe_hook import StripeHook


class StripeToS3Operator(BaseOperator, SkipMixin):
    """
    Stripe to S3 Operator

    :param stripe_conn_id:          Name of the Airflow connection that has
                                    your Stripe username, password and user_key
    :type stripe_conn_id:           String
    :param stripe_object:           Name of the Stripe object. Currently
                                    supported objects include:
                                        - BalanceTransaction
                                        - Charge
                                        - Coupon
                                        - Customer
                                        - Dispute
                                        - Event
                                        - FileUpload
                                        - Invoice
                                        - InvoiceItem
                                        - Payout
                                        - Order
                                        - OrderReturn
                                        - Plan
                                        - Product
                                        - Refund
                                        - SKU
                                        - Subscription
    :type stripe_object:            String
    :param stripe_args              *(optional)* Extra stripe arguments
    :type stripe_args:              Dictionary
    :param s3_conn_id:              Name of the S3 connection id
    :type s3_conn_id:               String
    :param s3_bucket:               name of the destination S3 bucket
    :type s3_bucket                 String
    :param s3_key:                  name of the destination file from bucket
    :type s3_key:                   String
    :param fields:                  *(optional)* list of fields that you want
                                    to get from the object.
                                    If *None*, then this will get all fields
                                    for the object
    :type fields:                   List
    :param replication_key_value:   *(optional)* value of the replication key,
                                    if needed. The operator will import only
                                    results with the id grater than the value of
                                    this param.
    :type replication_key_value:    String
    """

    template_fields = ("s3_key", "replication_key_value")

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

        super().__init__(*args, **kwargs)

        self.stripe_conn_id = stripe_conn_id
        self.stripe_object = stripe_object
        self.stripe_args = stripe_args
        self.s3_conn_id = s3_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.fields = fields
        self.replication_key_value = replication_key_value

    def execute(self, context):
        """
        Execute the operator.
        This will get all the data for a particular Stripe model
        and write it to a file.
        """
        logging.info("Prepping to gather data from Stripe")

        hook = StripeHook(conn_id=self.stripe_conn_id)

        logging.info("Making request for {0} object".format(self.stripe_object))

        results = hook.run_query(self.stripe_object,
                                 self.replication_key_value,
                                 **self.stripe_args)

    # Write the results to a temporary file and save that file to s3.
        with NamedTemporaryFile("w") as tmp:
            for result in results:
                filtered_result = self.filter_fields(result)
                tmp.write(json.dumps(filtered_result) + '\n')

            tmp.flush()

            if os.stat(tmp.name).st_size == 0:
                logging.info("No records pulled from Stripe.")
                downstream_tasks = context['task'].get_flat_relatives(
                    upstream=False)
                logging.info('Skipping downstream tasks...')
                logging.debug("Downstream task_ids %s", downstream_tasks)

                if downstream_tasks:
                    self.skip(context['dag_run'],
                              context['ti'].execution_date,
                              downstream_tasks)
                return True

            else:
                dest_s3 = S3Hook(self.s3_conn_id)
                dest_s3.load_file(
                    filename=tmp.name,
                    key=self.s3_key,
                    bucket_name=self.s3_bucket,
                    replace=True

                )

                tmp.close()

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

import stripe

from airflow.hooks.base_hook import BaseHook


class StripeHook(BaseHook):
    def __init__(self,
                 conn_id,
                 *args,
                 **kwargs):
        self.conn_id = conn_id
        self._args = args
        self._kwargs = kwargs

        self.connection = None
        self.extras = None
        self.stripe = None

    def get_conn(self):
        """
        Initialize a stripe instance.
        """
        if self.stripe:
            return self.stripe

        self.connection = self.get_connection(self.conn_id)
        self.extras = self.connection.extra_dejson

        stripe.api_key = self.extras.get('api_key', None)

        self.stripe = stripe

        return stripe

    def run_query(self,
                  stripe_object,
                  replication_key_value=None,
                  **kwargs):
        """
        Run a query against stripe
        :param stripe_object:           name of the Stripe object
        :param replication_key_value:   Stripe replicaton key value
        """
        stripe_instance = self.get_conn()
        stripe_endpoint = getattr(stripe_instance, stripe_object)

        method_to_call = 'list'
        if stripe_object is 'BalanceHistory':
            method_to_call = 'all'
        if replication_key_value:
            stripe_response = getattr(stripe_endpoint, method_to_call)(
                ending_before=replication_key_value, **kwargs)
        else:
            stripe_response = getattr(stripe_endpoint, method_to_call)(**kwargs)

        for res in stripe_response.auto_paging_iter():
            yield res

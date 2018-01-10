from airflow.plugins_manager import AirflowPlugin
from stripe_plugin.operators.stripe_to_s3_operator import StripeToS3Operator
from stripe_plugin.hooks.stripe_hook import StripeHook


class stripe_plugin(AirflowPlugin):
    name = "stripe_plugin"
    operators = [StripeToS3Operator]
    hooks = [StripeHook]
    # Leave in for explicitness
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []

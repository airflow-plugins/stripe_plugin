# Plugin - Stripe to S3

This plugin moves data from the [Stripe](https://stripe.com/docs/api) API to S3 based on the specified object

## Hooks
### StripeHook
This hook handles the authentication and request to Stripe. Based on [stripe-python](https://github.com/stripe/stripe-python) module.

### S3Hook
[Core Airflow S3Hook](https://pythonhosted.org/airflow/_modules/S3_hook.html) with the standard boto dependency.

## Operators
### StripeToS3Operator
This operator composes the logic for this plugin. It fetches the stripe specified object and saves the result in a S3 Bucket, under a specified key, in njson format. The parameters it can accept include the following.

- `stripe_conn_id`: The Stripe connection id from Airflow
- `stripe_object`: Stripe object to query. Tested for `BalanceTransaction`, `Charge`, `Coupon`, `Customer`, `Event`, `InvoiceItem`, `Invoice`, `Plan`, `Subscription`, `Transfer`
- `stripe_args`: *optional* dictionary with any extra arguments accepted by stripe-python module, 
- `s3_conn_id`: S3 connection id from Airflow.  
- `s3_bucket`: The output s3 bucket.  
- `s3_key`: The input s3 key.  
- `fields`: *optional* list of fields that you want to get from the object. If *None*, then this will get all fields for the object
- `replication_key_value`: *(optional)*  value of the replication key, if needed. The operator will import only the objects created after the object with this id.


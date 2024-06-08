from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
s3_sensor = S3KeySensor(
            task_id='wait_for_train_file',
            poke_interval=60,
            timeout=180,
            soft_fail=False,
            retries=2,
            bucket_key='train_auto.csv',
            bucket_name='train',
            aws_conn_id='aws_default')
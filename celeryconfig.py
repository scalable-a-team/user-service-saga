import os

main = 'tasks'
broker_url = os.getenv('CELERY_BROKER_URL', default='amqp://guest:guest@localhost:5672//')
result_backend = os.getenv('CELERY_RESULT_BACKEND')
task_acks_late = True
task_acks_on_failure_or_timeout = False
task_reject_on_worker_lost = True
task_serializer = 'pickle'
accept_content = ['pickle']

task_routes = {
    'tasks.reserve_buyer_credit': {'queue': 'user'},
    'tasks.refund_buyer': {'queue': 'user'},
    'tasks.transfer_to_seller_balance': {'queue': 'user'},
}

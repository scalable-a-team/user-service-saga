class OrderStatus:
    INIT = 'init'
    PENDING = 'pending'
    IN_PROGRESS = 'in_progress'
    SUCCESS = 'success'


class QueueName:
    ORDER = 'order'
    PRODUCT = 'product'


class EventStatus:
    RESERVE_BUYER_CREDIT = 'reserve_buyer_wallet'
    REVERT_RESERVE_BUYER_CREDIT = 'revert_reserve_buyer_wallet'
    REVERT_CREATE_ORDER = 'revert_create_order'
    APPROVE_ORDER_PENDING = 'approve_order_pending'
    TRANSFER_TO_SELLER_BALANCE = 'transfer_to_seller_balance'
    REFUND_BUYER = 'refund_buyer'

    _queue_mapping = {
        RESERVE_BUYER_CREDIT: 'user',
        APPROVE_ORDER_PENDING: 'order',
        REVERT_CREATE_ORDER: 'order',
        REVERT_RESERVE_BUYER_CREDIT: 'product',
    }

    @classmethod
    def get_queue(cls, name):
        return cls._queue_mapping[name]

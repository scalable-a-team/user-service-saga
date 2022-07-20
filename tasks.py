import decimal
import os

from celery import Celery
from celery.signals import worker_process_init
from celery.utils.log import get_task_logger
from opentelemetry import trace, propagate
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.celery import CeleryInstrumentor
from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.trace import TracerProvider
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, and_

import celeryconfig
from enums import EventStatus
from models import BuyerWallet, ProcessedEvent

DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
engine = create_engine(
    f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}',
    echo=True
)
Session = sessionmaker(bind=engine)

ProcessedEvent.__table__.create(engine, checkfirst=True)

tracer = None
PROPAGATOR = None

@worker_process_init.connect(weak=False)
def init_celery_tracing(*args, **kwargs):
    global tracer
    global PROPAGATOR
    resource = Resource.create(attributes={
        "service.name": "UserSagaWorker"
    })
    trace.set_tracer_provider(TracerProvider(resource=resource))
    span_processor = BatchSpanProcessor(
        OTLPSpanExporter(endpoint=os.getenv('OTEL_EXPORTER_OTLP_ENDPOINT'), insecure=True)
    )
    trace.get_tracer_provider().add_span_processor(span_processor)
    SQLAlchemyInstrumentor().instrument(engine=engine)
    CeleryInstrumentor().instrument()
    tracer = trace.get_tracer(__name__)
    PROPAGATOR = propagate.get_global_textmap()


app = Celery()
app.config_from_object(celeryconfig)
logger = get_task_logger(__name__)


@app.task(name=EventStatus.RESERVE_BUYER_CREDIT, bind=True)
def reserve_buyer_credit(self, buyer_id, product_id, order_id, seller_id, product_amount):
    decimal_product_amount = decimal.Decimal(product_amount)
    current_event = EventStatus.RESERVE_BUYER_CREDIT
    logger.info(f"Receive Buyer ID: {buyer_id}, Product ID: {product_id}, Order ID: {order_id}")
    db_session = Session()

    event_record = db_session.query(ProcessedEvent).filter(and_(
        ProcessedEvent.chain_id == order_id,
        ProcessedEvent.event == current_event,
    )).first()
    db_session.commit()

    payload = {
        'order_id': order_id,
        'product_id': product_id,
        'buyer_id': buyer_id,
        'seller_id': seller_id,
        'product_amount': product_amount
    }
    # If event is already processed, we skip the event processing
    # but fire the next event just in-case the next-published message is lost
    if event_record is not None:
        with tracer.start_span(name=f"send_task {event_record.next_event}"):
            app.send_task(
                event_record.next_event,
                kwargs=payload,
                queue=EventStatus.get_queue(event_record.next_event),
            )
            return payload

    transaction_success = False

    with tracer.start_span(name="Execute DB Transaction"):
        try:
            with db_session.begin():
                # Lock DB row
                buyer_wallet = db_session.query(BuyerWallet).with_for_update().filter_by(buyer_id=buyer_id).first()
                if buyer_wallet.balance < decimal_product_amount:
                    raise Exception(f"User balance not enough. User balance: {buyer_wallet.balance},"
                                    f" Product amount: {product_amount}")
                buyer_wallet.balance -= decimal_product_amount
                db_session.flush()
                history = ProcessedEvent(
                    chain_id=order_id,
                    event_id=self.request.id,
                    event=current_event,
                    next_event=EventStatus.APPROVE_ORDER_PENDING,
                    step=0
                )
                db_session.add(history)
            transaction_success = True
        except Exception as e:
            logger.error(e)
            logger.info(f"{current_event} failed for Buyer ID: {buyer_id} Product ID: {product_id}")

    if transaction_success:
        with tracer.start_span(name=f"send_task {EventStatus.APPROVE_ORDER_PENDING}"):
            app.send_task(
                EventStatus.APPROVE_ORDER_PENDING,
                kwargs=payload,
                queue=EventStatus.get_queue(EventStatus.APPROVE_ORDER_PENDING),
            )
    else:
        next_event = EventStatus.REVERT_CREATE_ORDER
        history = ProcessedEvent(
            chain_id=order_id,
            event_id=self.request.id,
            event=current_event,
            next_event=next_event,
            step=0
        )
        db_session.add(history)
        db_session.commit()
        with tracer.start_span(name=f"send_task {next_event}"):
            app.send_task(
                next_event,
                kwargs=payload,
                queue=EventStatus.get_queue(next_event),
            )
    return payload


@app.task(name=EventStatus.REFUND_BUYER, bind=True)
def refund_buyer(self, order_id, buyer_id, product_amount):
    decimal_product_amount = decimal.Decimal(product_amount)
    current_event = EventStatus.REFUND_BUYER
    logger.info(f"Receive Order ID: {order_id}")
    db_session = Session()

    event_record = db_session.query(ProcessedEvent).filter(and_(
        ProcessedEvent.chain_id == order_id,
        ProcessedEvent.event == current_event,
    )).first()
    db_session.commit()

    if event_record is not None:
        return


    with tracer.start_span(name="Execute DB Transaction"):
        try:
            with db_session.begin():
                # Lock DB row
                buyer_wallet = db_session.query(BuyerWallet).with_for_update().filter_by(buyer_id=buyer_id).first()
                buyer_wallet.balance += decimal_product_amount
                db_session.flush()
                history = ProcessedEvent(
                    chain_id=order_id,
                    event_id=self.request.id,
                    event=current_event,
                    next_event=None,
                    step=0
                )
                db_session.add(history)
        except Exception as e:
            logger.error(e)
            logger.info(f"{current_event} failed for Buyer ID: {buyer_id}")
            raise e


@app.task(name=EventStatus.TRANSFER_TO_SELLER_BALANCE, bind=True)
def transfer_to_seller_balance(self, order_id, seller_id, product_amount):
    decimal_product_amount = decimal.Decimal(product_amount)
    current_event = EventStatus.TRANSFER_TO_SELLER_BALANCE
    logger.info(f"Receive Order ID: {order_id}")
    db_session = Session()

    event_record = db_session.query(ProcessedEvent).filter(and_(
        ProcessedEvent.chain_id == order_id,
        ProcessedEvent.event == current_event,
    )).first()
    db_session.commit()

    if event_record is not None:
        return

    with tracer.start_span(name="Execute DB Transaction"):
        try:
            with db_session.begin():
                # Lock DB row
                seller_wallet = db_session.query(BuyerWallet).with_for_update().filter_by(seller_id=seller_id).first()
                seller_wallet.balance += decimal_product_amount
                db_session.flush()
                history = ProcessedEvent(
                    chain_id=order_id,
                    event_id=self.request.id,
                    event=current_event,
                    next_event=None,
                    step=0
                )
                db_session.add(history)
        except Exception as e:
            logger.error(e)
            logger.info(f"{current_event} failed for Order ID: {order_id}")
            raise e


def _header_from_carrier(carrier, key):
    header = carrier.get(key)
    return [header] if header else []

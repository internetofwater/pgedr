import os
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
)
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
    OTLPSpanExporter,
)
from opentelemetry.sdk.resources import Resource
import functools
import inspect

_otel_initialized = False


def init_otel():
    """Initialize the open telemetry config"""
    global _otel_initialized
    # Guard clause to prevent multiple initializations; should never be called;
    # here to prevent accidental double initialization in future
    assert not _otel_initialized, 'OpenTelemetry has already been initialized'
    _otel_initialized = True

    resource = Resource(
        attributes={'service.name': os.getenv('OTEL_SERVICE_NAME', 'pgedr')}
    )
    provider = TracerProvider(resource=resource)
    COLLECTOR_ENDPOINT = os.environ.get('COLLECTOR_ENDPOINT', '127.0.0.1')
    COLLECTOR_GRPC_PORT = os.environ.get('COLLECTOR_GRPC_PORT', 4317)

    processor = BatchSpanProcessor(
        OTLPSpanExporter(
            endpoint=f'http://{COLLECTOR_ENDPOINT}:{COLLECTOR_GRPC_PORT}'
        ),
        # reduce the runtime CPU effect by sending spans less frequently
        max_queue_size=2024,
        max_export_batch_size=512,
        schedule_delay_millis=5000,
    )
    provider.add_span_processor(processor)

    # Sets the global default tracer provider
    trace.set_tracer_provider(provider)

    print('Initialized open telemetry')


init_otel()
TRACER = trace.get_tracer('pgedr_tracer')


def otel_trace():
    """
    Decorator to automatically set the span name using the file
    and function name.
    """

    def decorator(func):
        filename = os.path.splitext(os.path.basename(inspect.getfile(func)))[0]

        # don't add __init__ to span names for the sake of reducing noise
        # i.e pgedr is a more useful name than pgedr.__init__
        prefix = '' if filename == '__init__' else f'{filename}.'
        span_name = f'{prefix}{func.__name__}'

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            with TRACER.start_as_current_span(span_name):
                return func(*args, **kwargs)

        return wrapper

    return decorator


def add_args_as_attributes_to_span():
    """
    Inspect the caller's frame and add all arguments (except `self`)
    as attributes on the current OpenTelemetry span; useful for
    debugging to see what arguments were passed to a function.
    """
    span = trace.get_current_span()

    # No active span? Nothing to do.
    if span is None or not span.is_recording():
        return

    try:
        # Grab the caller's frame
        current_frame = inspect.currentframe()
        if not current_frame:
            return

        callee_frame = current_frame.f_back
        if not callee_frame:
            return

        # Get local variables of the calling function
        args = callee_frame.f_locals

        func_name = callee_frame.f_code.co_name

        for name, value in args.items():
            if name == 'self':
                continue
            span.set_attribute(f'{func_name}.arg.{name}', str(value))
    except Exception:
        # Never make the app crash because of tracing
        pass

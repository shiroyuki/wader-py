from threading import Lock

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter

_tracing_enabler_lock = Lock()
_tracing_enabled = False


def enable_tracing():
    global _tracing_enabled

    with _tracing_enabler_lock:
        if _tracing_enabled:
            return

        provider = TracerProvider()
        processor = BatchSpanProcessor(ConsoleSpanExporter())
        provider.add_span_processor(processor)

        # Sets the global default tracer provider
        trace.set_tracer_provider(provider)

        _tracing_enabled = True
    # end lock
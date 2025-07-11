from typing import Collection
from functools import wraps

import peewee
from peewee import SENTINEL

from opentelemetry import trace
from opentelemetry.trace import SpanKind, Status, StatusCode
from opentelemetry.semconv.trace import SpanAttributes
from opentelemetry.instrumentation.instrumentor import BaseInstrumentor
from opentelemetry.instrumentation.peewee.package import _instruments
from opentelemetry.instrumentation.peewee.version import __version__


def _get_tracer(trace_provider=None):
    return trace.get_tracer(
        __name__,
        __version__,
        tracer_provider=trace_provider
    )

def _get_attributes_from_url(url):
    pass

def _get_attributes_from_connect_params(params):
    attrs = {}
    if 'host' in params:
        attrs[SpanAttributes.NET_HOST_NAME] =  params.get('host')
    if 'user' in params:
        attrs[SpanAttributes.DB_USER] = params.get('user')

    attrs[SpanAttributes.NET_HOST_PORT] = params.get('port', 3306)

    return attrs, bool('host' in params)

def _get_operation_name(vendor, db_name, sql):
    parts = []
    if isinstance(sql, str):
        parts.append(sql.split()[0])
    if db_name:
        parts.append(db_name)
    if not parts:
        return vendor
    return " ".join(parts)

def _normalize_vendor(vendor):

    if 'sqlite' in vendor.lower():
        return 'sqlite'

    if 'mysql' in vendor.lower():
        return 'mysql'

    if 'postgresql' in vendor.lower():
        return 'postgresql'

    raise ValueError

def _wrap_execute_sql(trace_provider=None):
    tracer = _get_tracer(trace_provider)

    original_execute = peewee.Database.execute_sql

    @wraps(original_execute)
    def execute_sql(self, sql, params=None, commit=SENTINEL):
        database = self.database
        vendor = _normalize_vendor(self.__class__.__name__)
        attrs, found = _get_attributes_from_connect_params(self.connect_params)
        if isinstance(database, str):
            attrs[SpanAttributes.DB_NAME] = database

        span = tracer.start_span(
            _get_operation_name(vendor, database, sql),
            kind=SpanKind.CLIENT
        )
        with trace.use_span(span, end_on_exit=True):
            if span.is_recording():
                span.set_attribute(SpanAttributes.DB_STATEMENT, sql)
                span.set_attribute(SpanAttributes.DB_SYSTEM, vendor)
                for key, value in attrs.items():
                    span.set_attribute(key,value)

            try:
                if span.is_recording():
                    span.set_status(
                        Status(
                            StatusCode.OK
                        )
                    )
                return original_execute(self, sql, params, commit)
            except Exception as exc:
                span.record_exception(exc)
                span.set_status(
                    Status(
                        StatusCode.ERROR,
                        str(exc)
                    )
                )
                raise

    return execute_sql

def _wrap_connect(trace_provider=None):
    tracer  = _get_tracer(trace_provider)

    original_connect = peewee.Database.connect

    @wraps(original_connect)
    def connect(self, reuse_if_open=False):
        with tracer.start_as_current_span(
            "connect", kind=SpanKind.CLIENT
        ) as span:
            try:
                return original_connect(self, reuse_if_open)
            except Exception as exc:
                if span.is_recording():
                    span.record_exception(exc)
                    span.set_status(
                        Status(
                            StatusCode.ERROR,
                            str(exc)
                        )
                    )
                raise




    return connect


class PeeweeInstrumentor(BaseInstrumentor):
    def instrumentation_dependencies(self) -> Collection[str]:
        return _instruments

    def _instrument(self, **kwargs):
        self.original_mysql_database = peewee.MySQLDatabase
        trace_provider = kwargs.get('trace_provider', None)

        peewee.Database.connect = _wrap_connect(trace_provider)
        peewee.Database.execute_sql = _wrap_execute_sql(trace_provider)


    def _uninstrument(self, **kwargs):
        pass
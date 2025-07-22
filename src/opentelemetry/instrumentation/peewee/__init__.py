import re
from functools import wraps
from timeit import default_timer
from typing import Collection

import peewee
from opentelemetry import trace, metrics
from opentelemetry.instrumentation.instrumentor import BaseInstrumentor
from opentelemetry.instrumentation.peewee.package import _instruments
from opentelemetry.instrumentation.peewee.version import __version__
from opentelemetry.instrumentation.sqlcommenter_utils import _add_sql_comment
from opentelemetry.instrumentation.utils import _get_opentelemetry_values
from opentelemetry.semconv.metrics import MetricInstruments
from opentelemetry.semconv.trace import SpanAttributes
from opentelemetry.trace import SpanKind, Status, StatusCode
from peewee import SENTINEL
from opentelemetry.util._importlib_metadata import version

peewee_version = version("peewee")

def _get_tracer(tracer_provider=None):
    return trace.get_tracer(
        __name__,
        __version__,
        tracer_provider=tracer_provider,
        schema_url="https://opentelemetry.io/schemas/1.11.0"
    )


def _get_meter(meter_provider=None):
    return metrics.get_meter(
        __name__,
        __version__,
        meter_provider=meter_provider,
        schema_url="https://opentelemetry.io/schemas/1.11.0"
    )


def _get_attributes_from_url(url):
    pass


def _get_attributes_from_connect_params(params):
    attrs = {}
    if 'host' in params:
        attrs[SpanAttributes.NET_HOST_NAME] = params.get('host')
    if 'user' in params:
        attrs[SpanAttributes.DB_USER] = params.get('user')

    attrs[SpanAttributes.NET_HOST_PORT] = params.get('port', 3306)

    return attrs, bool('host' in params)


def _get_operation_name(vendor, db_name, sql):
    parts = []
    if isinstance(sql, str):
        # otel spec recommends against parsing SQL queries. We are not trying to parse SQL
        # but simply truncating the statement to the first word. This covers probably >95%
        # use cases and uses the SQL statement in span name correctly as per the spec.
        # For some very special cases it might not record the correct statement if the SQL
        # dialect is too weird but in any case it shouldn't break anything.
        # Strip leading comments so we get the operation name.
        parts.append(
            re.compile(r'^/\*.*?\*/').sub("", sql).split()[0]
        )
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

def _get_commenter_data(self, commenter_options) -> dict:
    """Calculate sqlcomment contents from conn and configured options"""
    commenter_data = dict(
        db_driver=self.__class__.__name__,
        db_framework=f'peewee:{peewee_version}'
    )

    if commenter_options.get('opentelemetry_values', True):
        commenter_data.update(**_get_opentelemetry_values())

    # Filter down to just the requested attributes
    commenter_data = {
        k: v
        for k, v in commenter_data.items()
        if commenter_options.get(k, True)
    }
    return commenter_data

def _set_db_client_span_attributes(vendor, span, sql, attrs) -> None:
    """Uses sql and attrs to set attributes of provided Otel span"""
    span.set_attribute(SpanAttributes.DB_STATEMENT, sql)
    span.set_attribute(SpanAttributes.DB_SYSTEM, vendor)
    for key, value in attrs.items():
        span.set_attribute(key, value)

def _wrap_execute_sql(
        duration_histogram,
        tracer=None,
        enable_sqlcommenter=False,
        commenter_options=None,
        enable_attribute_commenter=False,
):
    original_execute = peewee.Database.execute_sql

    @wraps(original_execute)
    def execute_sql(self, sql, params=None, commit=SENTINEL):
        start = default_timer()
        database = self.database
        vendor = _normalize_vendor(self.__class__.__name__)
        attrs, found = _get_attributes_from_connect_params(self.connect_params)
        if isinstance(database, str):
            attrs[SpanAttributes.DB_NAME] = database

        span = tracer.start_span(
            _get_operation_name(vendor, database, sql),
            kind=SpanKind.CLIENT
        )
        duration_attrs = {
            "db.system.name": vendor,
            "db.query.text": sql
        }
        if SpanAttributes.NET_HOST_NAME in attrs:
            duration_attrs["db.server.address"] = attrs[SpanAttributes.NET_HOST_NAME]

        with trace.use_span(span, end_on_exit=True):
            if span.is_recording():
                span.set_attribute(SpanAttributes.DB_STATEMENT, sql)
                span.set_attribute(SpanAttributes.DB_SYSTEM, vendor)
                for key, value in attrs.items():
                    span.set_attribute(key, value)
            if enable_sqlcommenter:
                commenter_data = _get_commenter_data(self, commenter_options)

                if enable_attribute_commenter:
                    # just to handle type safety
                    sql = str(sql)

                    # sqlcomment is added to executed query and db.statement span attribute
                    sql = _add_sql_comment(
                        sql, **commenter_data
                    )

                    _set_db_client_span_attributes(vendor, span, sql, attrs)
                else:
                    # sqlcomment is only added to executed query
                    # so db.statement is set before add_sql_comment
                    _set_db_client_span_attributes(vendor, span, sql, attrs)
                    sql = _add_sql_comment(
                        sql, **commenter_data
                    )
            try:
                if span.is_recording():
                    span.set_status(
                        Status(
                            StatusCode.OK
                        )
                    )
                result = original_execute(self, sql, params, commit)
            except Exception as exc:
                span.record_exception(exc)
                span.set_status(
                    Status(
                        StatusCode.ERROR,
                        str(exc)
                    )
                )
                raise
            finally:
                duration_s = default_timer() - start
                duration_histogram.record(max(round(duration_s * 1000), 0), duration_attrs)
            return result
        return None

    return execute_sql


def _wrap_connect(tracer, active_connections):
    original_connect = peewee.Database.connect

    @wraps(original_connect)
    def connect(self, reuse_if_open=False):
        with tracer.start_as_current_span(
                "connect", kind=SpanKind.CLIENT
        ) as span:
            if span.is_recording():
                attrs, found = _get_attributes_from_connect_params(self.connect_params)
                span.set_attributes(attrs)
                span.set_attribute(
                    SpanAttributes.DB_SYSTEM, _normalize_vendor(self.__class__.__name__)
                )
            try:
                result = original_connect(self, reuse_if_open)
                _add_used_to_connection_usage(self, active_connections, 1)
                return result
            except Exception as exc:
                span.record_exception(exc)
                span.set_status(
                    Status(
                        StatusCode.ERROR,
                        str(exc)
                    )
                )
                raise
        return None

    return connect


def _wrap_close(active_connections):
    original_close = peewee.Database.close

    @wraps(original_close)
    def close(self):
        result = original_close(self)
        _add_used_to_connection_usage(self, active_connections, -1)
        return result

    return close


# Can't use this as peewee can't hook into checkin/checkout
def _add_idle_to_connection_usage(database, active_connections, value):
    active_connections.add(
        value,
        attributes={
            **_get_attributes_from_database(database),
            "state": "idle"
        }
    )


def _add_used_to_connection_usage(database, active_connections, value):
    active_connections.add(
        value,
        attributes={
            **_get_attributes_from_database(database),
            "state": "used"
        }
    )


def _get_connection_string(database):
    if hasattr(database, 'connect_params'):
        params = database.connect_params
        host = params.get('host')
        port = params.get('port', 3306)
        name = database.database

        if isinstance(database, peewee.SqliteDatabase):
            drivername = 'sqlite'
        elif isinstance(database, peewee.MySQLDatabase):
            drivername = 'mysql'
        elif isinstance(database, peewee.PostgresqlDatabase):
            drivername = 'postgresql'
        else:
            drivername = ''

        return f'{drivername}://{host}:{port}/{name}'
    if isinstance(database, peewee.SqliteDatabase):
        return f'sqlite://{database.database}'

    return ''


def _get_attributes_from_database(database):
    attrs = {}

    attrs["pool.name"] = _get_connection_string(database)

    return attrs


class PeeweeInstrumentor(BaseInstrumentor):
    def instrumentation_dependencies(self) -> Collection[str]:
        return _instruments

    def _instrument(self, **kwargs):
        self.original_connect = peewee.Database.connect
        self.original_execute = peewee.Database.execute_sql
        self.original_close = peewee.Database.close
        tracer_provider = kwargs.get('tracer_provider')
        meter_provider = kwargs.get('meter_provider')

        meter = _get_meter(meter_provider)

        connection_usage = meter.create_up_down_counter(
            name=MetricInstruments.DB_CLIENT_CONNECTIONS_USAGE,
            unit="connection",
            description="The number of connections that are currently in state described by the state attribute.",
        )
        duration_histogram = meter.create_histogram(
            name="db.client.operation.duration",
            unit="ms",
            description="The duration of the operation",
        )
        tracer = _get_tracer(tracer_provider)

        enable_commenter = kwargs.get('enable_commenter', False)
        commenter_options = kwargs.get('commenter_options', {})
        enable_attribute_commenter = kwargs.get('enable_attribute_commenter', False)

        peewee.Database.connect = _wrap_connect(tracer, connection_usage)
        peewee.Database.execute_sql = _wrap_execute_sql(tracer=tracer, duration_histogram=duration_histogram,
                                                        enable_sqlcommenter=enable_commenter,
                                                        commenter_options=commenter_options, enable_attribute_commenter=enable_attribute_commenter)
        peewee.Database.close = _wrap_close(connection_usage)

    def _uninstrument(self, **kwargs):
        peewee.Database.connect = self.original_connect
        peewee.Database.execute_sql = self.original_execute
        peewee.Database.close = self.original_close

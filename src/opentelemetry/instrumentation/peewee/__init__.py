from timeit import default_timer
from typing import Collection
from functools import wraps

import peewee
from peewee import SENTINEL

from opentelemetry import trace, metrics
from opentelemetry.trace import SpanKind, Status, StatusCode
from opentelemetry.semconv.trace import SpanAttributes
from opentelemetry.instrumentation.sqlcommenter_utils import _add_sql_comment
from opentelemetry.instrumentation.utils import _get_opentelemetry_values
from opentelemetry.instrumentation.instrumentor import BaseInstrumentor
from opentelemetry.instrumentation.peewee.package import _instruments
from opentelemetry.instrumentation.peewee.version import __version__


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


def _wrap_execute_sql(
        duration_histogram,
        tracer=None,
        enable_sqlcommenter=False,
        commenter_options=None
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
                commenter_data = dict(
                    db_driver=self.__class__.__name__,
                    db_framework=f'peewee:{__version__}'
                )

                if commenter_options.get('opentelemetry_values', True):
                    commenter_data.update(**_get_opentelemetry_values())

                # Filter down to just the requested attributes
                commenter_data = {
                    k: v
                    for k, v in commenter_data.items()
                    if commenter_options.get(k, True)
                }
                sql = _add_sql_comment(sql, **commenter_data)
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
def _add_idle_to_connection_usage(database,active_connections, value):
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

        active_connections = meter.create_up_down_counter(
            name="db.client.connection.count",
            unit="connection",
            description="The number of active connections",
        )
        duration_histogram = meter.create_histogram(
            name="db.client.operation.duration",
            unit="ms",
            description="The duration of the operation",
        )
        tracer = _get_tracer(tracer_provider)

        enable_commenter = kwargs.get('enable_commenter', False)
        commenter_options = kwargs.get('commenter_options', {})

        peewee.Database.connect = _wrap_connect(tracer, active_connections)
        peewee.Database.execute_sql = _wrap_execute_sql(tracer=tracer, duration_histogram=duration_histogram,
                                                        enable_sqlcommenter=enable_commenter,
                                                        commenter_options=commenter_options)
        peewee.Database.close = _wrap_close(active_connections)

    def _uninstrument(self, **kwargs):
        peewee.Database.connect = self.original_connect
        peewee.Database.execute_sql = self.original_execute
        peewee.Database.close = self.original_close

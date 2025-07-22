from unittest import mock

import pytest
import peewee

from opentelemetry import trace
from opentelemetry.instrumentation.peewee import PeeweeInstrumentor
from opentelemetry.semconv.trace import SpanAttributes
from opentelemetry.sdk.resources import Resource, ResourceAttributes
from opentelemetry.sdk.trace import TracerProvider, export
from opentelemetry.test.test_base import TestBase


class TestPeeweeInstrumentation(TestBase):
    @pytest.fixture(autouse=True)
    def inject_fixtures(self, caplog):
        self._caplog = caplog

    def tearDown(self):
        super().tearDown()
        PeeweeInstrumentor().uninstrument()

    def test_trace_integration(self):
        PeeweeInstrumentor().instrument()
        database = peewee.SqliteDatabase(":memory:")

        database.connect()
        database.execute_sql("SELECT 1 + 1")
        spans = self.memory_exporter.get_finished_spans()

        self.assertEqual(len(spans), 2)
        # first span - the connection to the db
        self.assertEqual(spans[0].name, "connect")
        self.assertEqual(spans[0].kind, trace.SpanKind.CLIENT)
        # second span - the query itself
        self.assertEqual(spans[1].name, "SELECT :memory:")
        self.assertEqual(spans[1].kind, trace.SpanKind.CLIENT)

    def test_instrument_two_databases(self):
        PeeweeInstrumentor().instrument()
        database1 = peewee.SqliteDatabase(":memory:")
        database2 = peewee.SqliteDatabase(":memory:")

        cnx_1 = database1.connect()
        database1.execute_sql("SELECT 1 + 1")
        cnx_2 = database2.connect()
        database2.execute_sql("SELECT 1 + 1")

        spans = self.memory_exporter.get_finished_spans()
        self.assertEqual(len(spans), 4)

    def test_instrumentation_db_connect(self):
        PeeweeInstrumentor().instrument()
        database = peewee.SqliteDatabase(":memory:")
        database.connect()

        spans = self.memory_exporter.get_finished_spans()
        self.assertEqual(len(spans), 1)

    def test_not_recording(self):
        mock_tracer = mock.Mock()
        mock_span = mock.Mock()
        mock_context = mock.Mock()
        mock_span.is_recording.return_value = False
        mock_context.__enter__ = mock.Mock(return_value=mock_span)
        mock_context.__exit__ = mock.Mock(return_value=None)
        mock_tracer.start_span.return_value = mock_context
        mock_tracer.start_as_current_span.return_value = mock_context
        with mock.patch("opentelemetry.trace.get_tracer") as tracer:
            tracer.return_value = mock_tracer
            PeeweeInstrumentor().instrument(
                tracer_provider=self.tracer_provider,
            )
            database = peewee.SqliteDatabase(":memory:")
            database.connect()
            database.execute_sql("SELECT 1 + 1")
            self.assertFalse(mock_span.is_recording())
            self.assertTrue(mock_span.is_recording.called)
            self.assertFalse(mock_span.set_attribute.called)
            self.assertFalse(mock_span.set_status.called)

    def test_db_overrides(self):
        PeeweeInstrumentor().instrument()
        from peewee import SqliteDatabase
        database = SqliteDatabase(":memory:")
        database.connect()
        database.execute_sql("SELECT 1 + 1")

        spans = self.memory_exporter.get_finished_spans()

        self.assertEqual(len(spans), 2)
        # first span - the connection to the db
        self.assertEqual(spans[0].name, "connect")
        self.assertEqual(spans[0].kind, trace.SpanKind.CLIENT)
        # second span - the query
        self.assertEqual(spans[1].name, "SELECT :memory:")
        self.assertEqual(spans[1].kind, trace.SpanKind.CLIENT)
        self.assertEqual(
            spans[1].instrumentation_scope.name,
            "opentelemetry.instrumentation.peewee",
        )

    def test_custom_tracer_provider(self):
        provider = TracerProvider(
            resource=Resource(
                {
                    ResourceAttributes.SERVICE_NAME: "test",
                    ResourceAttributes.DEPLOYMENT_ENVIRONMENT: "dev",
                    ResourceAttributes.SERVICE_VERSION: "1234"
                }
            )
        )
        provider.add_span_processor(
            export.SimpleSpanProcessor(self.memory_exporter)
        )

        PeeweeInstrumentor().instrument(tracer_provider=provider)
        from peewee import SqliteDatabase
        database = SqliteDatabase(":memory:")
        database.connect()
        database.execute_sql("SELECT 1 + 1")

        spans = self.memory_exporter.get_finished_spans()

        self.assertEqual(len(spans), 2)
        self.assertEqual(spans[0].resource.attributes[ResourceAttributes.SERVICE_NAME], "test")
        self.assertEqual(spans[0].resource.attributes[ResourceAttributes.DEPLOYMENT_ENVIRONMENT], "dev")
        self.assertEqual(spans[0].resource.attributes[ResourceAttributes.SERVICE_VERSION], "1234")

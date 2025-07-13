import pytest
import logging
from opentelemetry.test.test_base import TestBase

from src.opentelemetry.instrumentation.peewee import PeeweeInstrumentor


class TestPeeweeInstrumentationWithSQLCommenter(TestBase):
    @pytest.fixture(autouse=True)
    def inject_fixtures(self, caplog):
        self.caplog = caplog

    def tearDown(self):
        super().tearDown()
        PeeweeInstrumentor().uninstrument()

    def test_sqlcommenter_disabled(self):
        logging.getLogger("peewee").setLevel(logging.DEBUG)

        PeeweeInstrumentor().instrument()
        from peewee import SqliteDatabase
        database = SqliteDatabase(":memory:")
        database.connect()
        database.execute_sql("SELECT 1")

        print(self.caplog.records)

        self.assertEqual(self.caplog.records[-1].getMessage(), "('SELECT 1', None)")

    def test_sqlcommenter_enabled(self):
        PeeweeInstrumentor().instrument(
            tracer_provider=self.tracer_provider,
            enable_commenter=True,
            commenter_options={"db_framework": False}
        )
        from peewee import SqliteDatabase
        database = SqliteDatabase(":memory:")
        database.connect()
        database.execute_sql("SELECT 1")
        self.assertRegex(
            self.caplog.records[-1].getMessage(),
            r"SELECT 1 /\*db_driver='(.*)',traceparent='\d{1,2}-[a-zA-Z0-9_]{32}-[a-zA-Z0-9_]{16}-\d{1,2}'\*/",
        )



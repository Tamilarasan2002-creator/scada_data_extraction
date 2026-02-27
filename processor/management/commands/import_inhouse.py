import os
import pandas as pd
from django.core.management.base import BaseCommand
from django.db import connection
from django.conf import settings
from psycopg2.extras import execute_values


class Command(BaseCommand):
    help = "Optimized Inhouse SCADA Import (File / Folder / Year)"

    def add_arguments(self, parser):
        parser.add_argument("--file", type=str, help="Import single CSV file")
        parser.add_argument("--folder", type=str, help="Import full folder")
        parser.add_argument("--year", type=str, help="Import full year")

    # --------------------------------------------------
    # MAIN
    # --------------------------------------------------
    def handle(self, *args, **options):

        if options["file"]:
            self.import_single_file(options["file"])

        elif options["folder"]:
            self.import_folder(options["folder"])

        elif options["year"]:
            self.import_year(options["year"])

        else:
            self.stdout.write(
                self.style.ERROR("❌ Provide --file OR --folder OR --year")
            )

    # --------------------------------------------------
    # PATH HANDLER
    # --------------------------------------------------
    def get_absolute_path(self, path):
        if os.path.isabs(path):
            return path
        return os.path.join(settings.BASE_DIR, path)

    # --------------------------------------------------
    # SINGLE FILE
    # --------------------------------------------------
    def import_single_file(self, file_path):

        file_path = self.get_absolute_path(file_path)

        if not os.path.exists(file_path):
            self.stdout.write(
                self.style.ERROR(f"❌ File not found: {file_path}")
            )
            return

        self.process_csv(file_path)

    # --------------------------------------------------
    # FOLDER
    # --------------------------------------------------
    def import_folder(self, folder_name):

        base_path = self.get_absolute_path("Inhouse")
        folder_path = os.path.join(base_path, folder_name)

        if not os.path.exists(folder_path):
            self.stdout.write(
                self.style.ERROR(f"❌ Folder not found: {folder_path}")
            )
            return

        for file in os.listdir(folder_path):
            if file.endswith(".csv"):
                self.process_csv(os.path.join(folder_path, file))

    # --------------------------------------------------
    # YEAR
    # --------------------------------------------------
    def import_year(self, year):

        base_path = self.get_absolute_path("Inhouse")

        for folder in os.listdir(base_path):
            if year in folder:
                folder_path = os.path.join(base_path, folder)

                for file in os.listdir(folder_path):
                    if file.endswith(".csv"):
                        self.process_csv(os.path.join(folder_path, file))

    # --------------------------------------------------
    # CORE PROCESSOR
    # --------------------------------------------------
    def process_csv(self, file_path):

        self.stdout.write(self.style.SUCCESS(f"\n🚀 Processing: {file_path}"))

        try:
            df = pd.read_csv(file_path)
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"❌ CSV read failed: {e}"))
            return

        # --------------------------------------------------
        # NORMALIZE COLUMN NAMES
        # --------------------------------------------------
        df.columns = df.columns.str.strip().str.lower()

        # Mapping dictionary (case insensitive safe)
        column_map = {
            "date": "timestamp",
            "asset name": "asset_name",
            "activepowergeneration": "active_power_generation",
            "windspeedoutsidenacelle": "windspeed_outside_nacelle",
            "temperatureoutsidenacelle": "temperature_outside_nacelle",
            "winddirectionoutsidenacelle": "winddirection_outside_nacelle",
        }

        file_columns = set(df.columns)
        expected_columns = set(column_map.keys())

        extra_columns = file_columns - expected_columns
        if extra_columns:
            self.stdout.write(
                self.style.WARNING(f"⚠ Extra columns: {extra_columns}")
            )

        missing_columns = expected_columns - file_columns
        if missing_columns:
            self.stdout.write(
                self.style.ERROR(f"❌ Missing columns: {missing_columns}")
            )
            return

        # Rename
        df = df.rename(columns=column_map)

        # --------------------------------------------------
        # CLEAN DATA
        # --------------------------------------------------
        df = df.dropna(how="all")

        df["timestamp"] = pd.to_datetime(
            df["timestamp"],
            format="%d-%m-%Y %H:%M:%S",
            errors="coerce"
        )
        df = df.dropna(subset=["timestamp", "asset_name"])

        if df.empty:
            self.stdout.write(
                self.style.WARNING("⚠ No valid data found")
            )
            return

        df = df.where(pd.notnull(df), None)

        records = list(df.itertuples(index=False, name=None))

        insert_query = """
            INSERT INTO inhouse_scada_data (
                timestamp,
                asset_name,
                active_power_generation,
                windspeed_outside_nacelle,
                temperature_outside_nacelle,
                winddirection_outside_nacelle
            )
            VALUES %s
            ON CONFLICT (timestamp, asset_name)
            DO UPDATE SET
                active_power_generation = EXCLUDED.active_power_generation,
                windspeed_outside_nacelle = EXCLUDED.windspeed_outside_nacelle,
                temperature_outside_nacelle = EXCLUDED.temperature_outside_nacelle,
                winddirection_outside_nacelle = EXCLUDED.winddirection_outside_nacelle
        """

        try:
            with connection.cursor() as cursor:
                execute_values(cursor, insert_query, records, page_size=10000)

            self.stdout.write(
                self.style.SUCCESS(f"✅ Inserted/Updated {len(records)} rows")
            )

        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f"❌ Database error: {e}")
            )
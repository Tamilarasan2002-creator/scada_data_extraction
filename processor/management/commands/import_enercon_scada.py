import os
import pandas as pd
from django.core.management.base import BaseCommand
from django.conf import settings
from django.utils import timezone
from django.db import connection
from psycopg2.extras import execute_values


class Command(BaseCommand):
    help = "Enercon SCADA Import (INSERT + UPDATE) - Production UPSERT"

    def add_arguments(self, parser):
        parser.add_argument("folder_name", type=str)
        parser.add_argument("--year", type=str)
        parser.add_argument("--file", type=str)

    def handle(self, *args, **kwargs):

        folder_name = kwargs["folder_name"]
        selected_year = kwargs.get("year")
        selected_file = kwargs.get("file")

        base_path = os.path.join(settings.BASE_DIR, folder_name)

        if selected_year:
            base_path = os.path.join(base_path, selected_year)

        if not os.path.exists(base_path):
            self.stdout.write(self.style.ERROR("❌ Folder not found"))
            return

        batch_size = 20000   # Increased for better performance
        chunk_size = 20000
        total_processed = 0

        for root, dirs, files in os.walk(base_path):

            for file in files:

                if not (file.endswith(".csv") or file.endswith(".xlsx")):
                    continue

                if selected_file and file != selected_file:
                    continue

                file_path = os.path.join(root, file)

                self.stdout.write(
                    self.style.SUCCESS(f"\n🚀 Processing: {file_path}")
                )

                if file.endswith(".csv"):
                    reader = pd.read_csv(file_path, chunksize=chunk_size)
                else:
                    df = pd.read_excel(file_path)
                    reader = [df]

                for chunk in reader:

                    chunk.columns = [
                        "date",
                        "asset_name",
                        "active_power_generation",
                        "wind_direction_outside_nacelle",
                        "wind_speed_outside_nacelle",
                        "temperature_outside_nacelle",
                    ]

                    # Convert date format (vectorized)
                    chunk["date"] = pd.to_datetime(
                        chunk["date"],
                        format="%d-%m-%Y %H:%M:%S",
                        errors="coerce"
                    )

                    # Drop invalid dates
                    chunk = chunk.dropna(subset=["date"])

                    # Make timezone aware (vectorized)
                    if timezone.is_naive(chunk["date"].iloc[0]):
                        chunk["date"] = chunk["date"].apply(
                            lambda x: timezone.make_aware(x)
                        )

                    # Create records using vectorized zip (FAST)
                    records = list(zip(
                        chunk["date"],
                        chunk["asset_name"],
                        chunk["active_power_generation"],
                        chunk["wind_direction_outside_nacelle"],
                        chunk["wind_speed_outside_nacelle"],
                        chunk["temperature_outside_nacelle"],
                    ))

                    # Process in batches
                    for i in range(0, len(records), batch_size):
                        batch = records[i:i + batch_size]
                        self.bulk_upsert(batch)
                        total_processed += len(batch)
                        self.stdout.write(
                            f"✅ Processed batch of {len(batch)} rows"
                        )

                self.stdout.write(
                    self.style.SUCCESS(f"🎯 Finished {file}")
                )

        self.stdout.write(
            self.style.SUCCESS(
                f"\n🔥 Import Completed! Total Rows Processed: {total_processed}"
            )
        )

    def bulk_upsert(self, records):

        query = """
        INSERT INTO scada_data_enercon (
            date,
            asset_name,
            active_power_generation,
            wind_direction_outside_nacelle,
            wind_speed_outside_nacelle,
            temperature_outside_nacelle
        )
        VALUES %s
        ON CONFLICT (asset_name, date)
        DO UPDATE SET
            active_power_generation = EXCLUDED.active_power_generation,
            wind_direction_outside_nacelle = EXCLUDED.wind_direction_outside_nacelle,
            wind_speed_outside_nacelle = EXCLUDED.wind_speed_outside_nacelle,
            temperature_outside_nacelle = EXCLUDED.temperature_outside_nacelle;
        """

        with connection.cursor() as cursor:
            execute_values(
                cursor,
                query,
                records,
                page_size=5000
            )
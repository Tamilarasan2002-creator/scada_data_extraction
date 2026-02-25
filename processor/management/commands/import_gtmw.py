import os
import pandas as pd
from django.core.management.base import BaseCommand
from django.conf import settings
from django.utils import timezone
from django.db import connection
from psycopg2.extras import execute_values


class Command(BaseCommand):
    help = "Import GTMW 10-Minute Data (Fast UPSERT)"

    def add_arguments(self, parser):
        parser.add_argument("folder_name", type=str)
        parser.add_argument("--file", type=str)

    def handle(self, *args, **kwargs):

        folder_name = kwargs["folder_name"]
        selected_file = kwargs.get("file")

        base_path = os.path.join(settings.BASE_DIR, folder_name)

        if not os.path.exists(base_path):
            self.stdout.write(self.style.ERROR("❌ Folder not found"))
            return

        batch_size = 20000
        total_processed = 0

        for root, dirs, files in os.walk(base_path):

            for file in files:

                if not file.endswith(".xlsx"):
                    continue

                if selected_file and file != selected_file:
                    continue

                file_path = os.path.join(root, file)

                self.stdout.write(
                    self.style.SUCCESS(f"\n🚀 Processing: {file_path}")
                )

                try:
                    df = pd.read_excel(file_path)

                    # Handle column variations safely
                    if len(df.columns) == 7:
                        # Device | Date | Quality | Misalignment | Active | Temp | Wind
                        df = df.iloc[:, [0, 1, 3, 4, 5, 6]]

                    elif len(df.columns) >= 6:
                        df = df.iloc[:, :6]

                    else:
                        self.stdout.write(
                            self.style.ERROR(f"❌ Skipping invalid format: {file}")
                        )
                        continue

                    df.columns = [
                        "device",
                        "time_only",
                        "misalignment_percent",
                        "avg_active_power",
                        "avg_ambient_temperature",
                        "avg_wind_speed",
                    ]

                    # Convert datetime
                    df["date"] = pd.to_datetime(
                        df["time_only"],
                        errors="coerce"
                    )

                    df = df.dropna(subset=["date"])

                    # Make timezone aware
                    if not df.empty and timezone.is_naive(df["date"].iloc[0]):
                        df["date"] = df["date"].apply(
                            lambda x: timezone.make_aware(x)
                        )

                    # Create records
                    records = list(zip(
                        df["device"],
                        df["date"],
                        df["misalignment_percent"],
                        df["avg_active_power"],
                        df["avg_ambient_temperature"],
                        df["avg_wind_speed"],
                    ))

                    # Batch insert
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

                except Exception as e:
                    self.stdout.write(
                        self.style.ERROR(f"❌ Error processing {file}: {e}")
                    )
                    continue

        self.stdout.write(
            self.style.SUCCESS(
                f"\n🔥 Import Completed! Total Rows Processed: {total_processed}"
            )
        )

    def bulk_upsert(self, records):

        query = """
        INSERT INTO gtmw (
            device,
            date,
            misalignment_percent,
            avg_active_power,
            avg_ambient_temperature,
            avg_wind_speed
        )
        VALUES %s
        ON CONFLICT (device, date)
        DO UPDATE SET
            misalignment_percent = EXCLUDED.misalignment_percent,
            avg_active_power = EXCLUDED.avg_active_power,
            avg_ambient_temperature = EXCLUDED.avg_ambient_temperature,
            avg_wind_speed = EXCLUDED.avg_wind_speed;
        """

        with connection.cursor() as cursor:
            execute_values(
                cursor,
                query,
                records,
                page_size=5000
            )
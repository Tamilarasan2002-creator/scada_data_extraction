import os
import time
import pandas as pd
from django.core.management.base import BaseCommand
from django.conf import settings


class Command(BaseCommand):
    help = "Convert XLSX to CSV year-wise without modifying rows or columns"

    def add_arguments(self, parser):
        parser.add_argument(
            "year",
            type=str,
            help="Enter year (example: 2023)"
        )

    def handle(self, *args, **options):

        start_time = time.time()

        year = options["year"].strip()
        report_path = os.path.join(settings.BASE_DIR, "reports")

        self.stdout.write(self.style.NOTICE(f"🔍 Searching for year {year} files..."))

        if not os.path.exists(report_path):
            self.stdout.write(
                self.style.ERROR(f"Reports folder not found: {report_path}")
            )
            return

        converted = False

        files = os.listdir(report_path)

        if not files:
            self.stdout.write(self.style.WARNING("⚠ Reports folder is empty"))
            return

        for file in files:

            if file.lower().endswith(".xlsx") and year in file:

                converted = True
                xlsx_path = os.path.join(report_path, file)
                csv_name = file.replace(".xlsx", ".csv")
                csv_path = os.path.join(report_path, csv_name)

                self.stdout.write(f"📂 Found file: {file}")
                self.stdout.write("⏳ Reading Excel file... Please wait...")

                try:
                    df = pd.read_excel(xlsx_path, dtype=str)

                    self.stdout.write("💾 Writing CSV file...")

                    df.to_csv(csv_path, index=False)

                    self.stdout.write(
                        self.style.SUCCESS(f"✅ Converted: {file} → {csv_name}")
                    )

                except Exception as e:
                    self.stdout.write(
                        self.style.ERROR(f"❌ Error converting {file}: {str(e)}")
                    )

        if not converted:
            self.stdout.write(
                self.style.WARNING(f"⚠ No file found for year {year}")
            )

        end_time = time.time()
        total_time = round(end_time - start_time, 2)

        self.stdout.write(self.style.SUCCESS(f"🚀 Process completed in {total_time} seconds"))
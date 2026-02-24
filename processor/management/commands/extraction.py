import os
import pandas as pd
from django.core.management.base import BaseCommand
from django.utils.timezone import make_aware, is_naive
from django.utils import timezone
from processor.models import SCADAData


class Command(BaseCommand):

    help = "Extract SCADA Excel files to Database"

    INPUT_FOLDER = "2022"   # ⚠ Change when needed

    def add_arguments(self, parser):
        parser.add_argument(
            "filename",
            nargs="?",
            type=str,
            help="Optional: Single Excel filename to process",
        )

    def handle(self, *args, **options):

        if options.get("filename"):
            file_name = options["filename"]

            if os.path.exists(file_name):
                full_path = file_name
            else:
                full_path = os.path.join(self.INPUT_FOLDER, file_name)

            if not os.path.exists(full_path):
                self.stdout.write(self.style.ERROR("❌ File not found!"))
                return

            self.process_file(full_path)

        else:
            if not os.path.exists(self.INPUT_FOLDER):
                self.stdout.write(
                    self.style.ERROR(f"❌ Input folder '{self.INPUT_FOLDER}' not found!")
                )
                return

            for file_name in os.listdir(self.INPUT_FOLDER):
                if file_name.endswith(".xlsx"):
                    full_path = os.path.join(self.INPUT_FOLDER, file_name)
                    self.process_file(full_path)

            self.stdout.write(self.style.SUCCESS("\n🎉 All files processed!"))

    def process_file(self, file_path):

        self.stdout.write(f"\n📂 Processing: {os.path.basename(file_path)}")

        try:
            xls = pd.ExcelFile(file_path)
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Error opening Excel file: {e}"))
            return

        batch_size = 5000
        batch = []
        debug_print_count = 0

        for sheet in xls.sheet_names:

            if sheet.lower() == "legend":
                continue

            try:
                raw_df = pd.read_excel(
                    file_path, sheet_name=sheet, header=None, nrows=10
                )

                header_row = None
                for i in range(len(raw_df)):
                    val = str(raw_df.iloc[i, 0]).lower()
                    if "local" in val:
                        header_row = i
                        break

                if header_row is None:
                    self.stdout.write(
                        self.style.WARNING(f"⚠ Header not found in sheet {sheet}")
                    )
                    continue

                df = pd.read_excel(
                    file_path, sheet_name=sheet, header=header_row
                )

                if df.empty:
                    continue

                datetime_col = df.columns[0]
                columns = list(df.columns)

                locno_map = {}
                i = 1

                while i + 4 < len(columns):
                    header = str(columns[i])
                    parts = header.split()

                    if len(parts) >= 2:
                        locno = parts[1]
                        locno_map[locno] = {
                            "Outdoor_Temp": columns[i],
                            "Wind_Speed": columns[i+1],
                            "Nacelle_Pos": columns[i+2],
                            "Active_Power": columns[i+3],
                            "frequency": columns[i+4],
                        }
                        i += 5
                    else:
                        i += 1

                self.stdout.write(f"Detected Locnos in {sheet}: {list(locno_map.keys())}")

                sheet_timestamp_count = 0

                for _, row in df.iterrows():

                    dt_val = row[datetime_col]

                    if pd.isna(dt_val):
                        continue

                    # 🔥 Strong datetime parsing
                    dt_obj = pd.to_datetime(
                        dt_val,
                        errors="coerce",
                        dayfirst=False
                    )

                    if pd.isna(dt_obj):
                        continue

                    # Remove seconds & microseconds (important)
                    dt_obj = dt_obj.replace(second=0, microsecond=0)

                    # Ensure timezone aware
                    if is_naive(dt_obj):
                        dt_obj = make_aware(dt_obj, timezone.get_current_timezone())

                    sheet_timestamp_count += 1

                    # Debug print first 10 timestamps only
                    if debug_print_count < 10:
                        self.stdout.write(
                            self.style.WARNING(f"DEBUG TIMESTAMP: {dt_obj}")
                        )
                        debug_print_count += 1

                    for locno, params in locno_map.items():

                        def to_float(val):
                            try:
                                return float(val)
                            except:
                                return 0.0

                        rec = SCADAData(
                            locno=locno,
                            datetime=dt_obj,
                            outdoor_temp=to_float(row[params["Outdoor_Temp"]]),
                            wind_speed=to_float(row[params["Wind_Speed"]]),
                            nacelle_pos=to_float(row[params["Nacelle_Pos"]]),
                            active_power=to_float(row[params["Active_Power"]]),
                            frequency=to_float(row[params["frequency"]]),
                        )

                        batch.append(rec)

                    if len(batch) >= batch_size:
                        self.save_batch(batch)
                        batch = []

                self.stdout.write(
                    self.style.SUCCESS(
                        f"✔ Sheet {sheet} processed with {sheet_timestamp_count} timestamps"
                    )
                )

            except Exception as e:
                self.stdout.write(
                    self.style.ERROR(f"Error processing sheet {sheet}: {e}")
                )

        if batch:
            self.save_batch(batch)

    def save_batch(self, batch):
        if not batch:
            return

        try:
            SCADAData.objects.bulk_create(
                batch,
                update_conflicts=True,
                unique_fields=["datetime", "locno"],
                update_fields=[
                    "outdoor_temp",
                    "wind_speed",
                    "nacelle_pos",
                    "active_power",
                    "frequency",
                ],
            )

            self.stdout.write(
                self.style.SUCCESS(f"✅ Saved batch of {len(batch)} records.")
            )

        except Exception as e:
            self.stdout.write(self.style.ERROR(f"❌ Batch save failed: {e}"))

from django.core.management.base import BaseCommand
from django.conf import settings
from django.utils import timezone
from datetime import datetime
from openpyxl import Workbook
from openpyxl.styles import Font
import requests
import os
from collections import defaultdict
from tqdm import tqdm

from processor.models import SCADAData


class Command(BaseCommand):
    help = "Generate SCADA Excel Report for Full Year (Optimized)"

    def add_arguments(self, parser):
        parser.add_argument("--year", type=int, required=True)

    def handle(self, *args, **options):

        year = options["year"]

        start = timezone.make_aware(datetime(year, 1, 1))
        end = timezone.make_aware(datetime(year + 1, 1, 1))

        self.stdout.write(f"\n🚀 Generating SCADA report for year {year}...\n")

        # --------------------------------------------------
        # 1️⃣ FETCH ALL YEAR DATA (ONLY ONE QUERY)
        # --------------------------------------------------
        queryset = (
            SCADAData.objects
            .filter(datetime__gte=start, datetime__lt=end)
            .order_by("datetime")
        )

        total_records = queryset.count()

        if total_records == 0:
            self.stdout.write(self.style.WARNING("No data found"))
            return

        self.stdout.write(f"📊 Total DB rows fetched: {total_records}\n")

        # --------------------------------------------------
        # 2️⃣ BUILD FAST LOOKUP DICTIONARY
        #    Structure:
        #    data_map[datetime][locno] = record
        # --------------------------------------------------
        data_map = defaultdict(dict)

        for record in tqdm(queryset.iterator(), 
                           total=total_records,
                           desc="Loading DB Data",
                           unit="rows"):

            data_map[record.datetime][record.locno] = record

        timestamps = sorted(data_map.keys())

        self.stdout.write(f"\n🕒 Total unique timestamps: {len(timestamps)}\n")

        # --------------------------------------------------
        # 3️⃣ FETCH MACHINE MASTER API
        # --------------------------------------------------
        machine_api_url = "http://172.16.7.118:8003/api/obs/leaplocs.php"
        machine_data = requests.get(machine_api_url).json()

        machines = list(machine_data.keys())

        parameters = [
            ("wind_speed", "m/s"),
            ("active_power", "kW"),
            ("outdoor_temp", "°C"),
            ("frequency", "Hz"),
            ("nacelle_pos", "°"),
        ]

        # --------------------------------------------------
        # 4️⃣ CREATE EXCEL
        # --------------------------------------------------
        wb = Workbook()
        ws = wb.active
        ws.title = f"SCADA {year}"

        bold = Font(bold=True)

        headers = [
            "Latitude",
            "Longitude",
            "LocNo",
            "Machine",
            "Parameter",
            "Units",
            "Datetime"
        ]

        for i, h in enumerate(headers, start=1):
            ws.cell(row=i, column=1, value=h).font = bold

        col = 2

        # Header values
        for locno in machines:
            for param, unit in parameters:

                ws.cell(row=1, column=col, value=machine_data[locno]["latitude"])
                ws.cell(row=2, column=col, value=machine_data[locno]["longitude"])
                ws.cell(row=3, column=col, value=locno)
                ws.cell(row=4, column=col, value=machine_data[locno]["machine"])
                ws.cell(row=5, column=col, value=param)
                ws.cell(row=6, column=col, value=unit)

                col += 1

        # --------------------------------------------------
        # 5️⃣ WRITE EXCEL DATA (NO DB QUERIES HERE)
        # --------------------------------------------------
        excel_row = 7

        for ts in tqdm(timestamps, desc="Writing Excel", unit="timestamps"):

            ws.cell(row=excel_row, column=1, value=ts.strftime("%Y-%m-%d %H:%M"))

            col = 2

            for locno in machines:

                record = data_map[ts].get(locno)

                for param, _ in parameters:
                    value = getattr(record, param) if record else None
                    ws.cell(row=excel_row, column=col, value=value)
                    col += 1

            excel_row += 1

        # --------------------------------------------------
        # 6️⃣ SAVE FILE
        # --------------------------------------------------
        folder = os.path.join(settings.BASE_DIR, "reports")
        os.makedirs(folder, exist_ok=True)

        path = os.path.join(folder, f"scada_report_{year}.xlsx")
        wb.save(path)

        self.stdout.write(self.style.SUCCESS(f"\n✅ Report Generated: {path}\n"))

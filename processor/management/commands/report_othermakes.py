import os
import requests
import pandas as pd
from datetime import datetime
from django.core.management.base import BaseCommand
from django.db import connection
from django.conf import settings


API_URL = "http://172.16.7.118:8003/api/obs/leaplocs.php"


class Command(BaseCommand):
    help = "Generate SCADA Report (Daily or Full Year - Single File)"

    # -----------------------------------------------------
    # ARGUMENTS
    # -----------------------------------------------------
    def add_arguments(self, parser):
        parser.add_argument("--date", type=str, help="Filter by date YYYY-MM-DD")
        parser.add_argument("--year", type=int, help="Generate full year report YYYY")

    # -----------------------------------------------------
    # SAFE DATABASE FETCH
    # -----------------------------------------------------
    def fetch_data(self, query, params):
        with connection.cursor() as cursor:
            cursor.execute(query, params)
            columns = [col[0] for col in cursor.description]
            rows = cursor.fetchall()
        return pd.DataFrame(rows, columns=columns)

    # -----------------------------------------------------
    # MERGED DATA FETCH (UNION ALL)
    # -----------------------------------------------------
    def fetch_all_data(self, report_date=None, report_year=None):

        if report_year:
            start_dt = f"{report_year}-01-01 00:00:00"
            end_dt = f"{report_year}-12-31 23:59:59"
        else:
            start_dt = f"{report_date} 00:00:00"
            end_dt = f"{report_date} 23:59:59"

        master_index = pd.date_range(start=start_dt, end=end_dt, freq="10min")

        query = """
            SELECT
                datetime,
                TRIM(UPPER(asset_name)) AS locno_key,
                active_power_generation,
                windspeed_outside_nacelle,
                temperature_outside_nacelle,
                winddirection_outside_nacelle
            FROM (

                SELECT
                    timestamp AS datetime,
                    asset_name,
                    active_power_generation,
                    windspeed_outside_nacelle,
                    temperature_outside_nacelle,
                    winddirection_outside_nacelle
                FROM inhouse_scada_data

                UNION ALL

                SELECT
                    date AS datetime,
                    device AS asset_name,
                    avg_active_power AS active_power_generation,
                    avg_wind_speed AS windspeed_outside_nacelle,
                    avg_ambient_temperature AS temperature_outside_nacelle,
                    misalignment_percent AS winddirection_outside_nacelle
                FROM gtmw

                UNION ALL

                SELECT
                    date AS datetime,
                    asset_name,
                    active_power_generation,
                    wind_speed_outside_nacelle AS windspeed_outside_nacelle,
                    temperature_outside_nacelle,
                    wind_direction_outside_nacelle AS winddirection_outside_nacelle
                FROM scada_data_enercon

            ) combined
            WHERE datetime BETWEEN %s AND %s
        """

        df = self.fetch_data(query, [start_dt, end_dt])

        if df.empty:
            return {}, master_index

        df["datetime"] = pd.to_datetime(df["datetime"])

        data_map = {}

        for locno, group in df.groupby("locno_key"):

            group = group.sort_values("datetime").set_index("datetime")

            # Remove duplicate timestamps
            group = group[~group.index.duplicated(keep="first")]

            # Align with master 10-min index
            group = group.reindex(master_index)

            data_map[locno] = group

        return data_map, master_index

    # -----------------------------------------------------
    # MAIN HANDLE
    # -----------------------------------------------------
    def handle(self, *args, **kwargs):

        report_date = kwargs.get("date")
        report_year = kwargs.get("year")

        if report_year:
            self.stdout.write(f"🚀 Generating FULL YEAR Report for {report_year}")
            all_data_map, global_time_index = self.fetch_all_data(
                report_year=report_year
            )
            report_label = str(report_year)
        else:
            if not report_date:
                report_date = datetime.now().strftime("%Y-%m-%d")

            self.stdout.write(f"🚀 Generating Daily Report for {report_date}")
            all_data_map, global_time_index = self.fetch_all_data(
                report_date=report_date
            )
            report_label = report_date

        # -----------------------------------------------------
        # FETCH API MACHINE LIST
        # -----------------------------------------------------
        self.stdout.write("🚀 Fetching API Machine List...")
        try:
            response = requests.get(API_URL, timeout=10)
            api_data = response.json()
        except Exception as e:
            self.stdout.write(f"❌ API Error: {str(e)}")
            return

        if not api_data:
            self.stdout.write("❌ No API data")
            return

        headers = {
            "lat": ["DateTime / Latitude"],
            "lon": ["Longitude"],
            "locno": ["Loc No"],
            "mac": ["Mac No"],
            "param": ["Parameters"],
            "unit": ["Units"],
        }

        machine_dataframes = []
        machine_no = 1

        structure = [
            ("temperature_outside_nacelle", "outdoor_temp", "°C"),
            ("windspeed_outside_nacelle", "wind_speed", "m/s"),
            ("winddirection_outside_nacelle", "wind_direction", "degree"),
        ]

        for item in api_data:
            locno = item.get("locno")
            lat = item.get("latitude")
            lon = item.get("longitude")

            locno_upper = locno.strip().upper()

            df = all_data_map.get(locno_upper)

            if df is None:
                df = pd.DataFrame(index=global_time_index)

            machine_label = f"M_{machine_no}"

            for col, pname, unit in structure:
                headers["lat"].append(lat)
                headers["lon"].append(lon)
                headers["locno"].append(locno)
                headers["mac"].append(machine_label)
                headers["param"].append(pname)
                headers["unit"].append(unit)

                if col in df.columns:
                    machine_dataframes.append(df[col])
                else:
                    machine_dataframes.append(
                        pd.Series([None] * len(global_time_index), index=global_time_index)
                    )

            machine_no += 1

        if not machine_dataframes:
            self.stdout.write("❌ No data structures built")
            return

        self.stdout.write("📊 Finalizing report structure...")

        final_data_df = pd.concat(machine_dataframes, axis=1)

        full_report = [
            headers["lat"],
            headers["lon"],
            headers["locno"],
            headers["mac"],
            headers["param"],
            headers["unit"],
        ]

        time_stamps = global_time_index.strftime("%Y-%m-%d %H:%M:%S").tolist()
        data_values = final_data_df.values.tolist()

        for i, row in enumerate(data_values):
            full_report.append([time_stamps[i]] + row)

        # -----------------------------------------------------
        # SAVE REPORT
        # -----------------------------------------------------
        base_reports_dir = os.path.join(settings.BASE_DIR, "make_reports")
        os.makedirs(base_reports_dir, exist_ok=True)

        if report_year:
            yearly_dir = os.path.join(
                base_reports_dir, "yearly_reports", str(report_year)
            )
            os.makedirs(yearly_dir, exist_ok=True)
            file_path = os.path.join(
                yearly_dir, f"scada_report_{report_year}.csv"
            )
        else:
            daily_dir = os.path.join(
                base_reports_dir, "daily_reports", report_label
            )
            os.makedirs(daily_dir, exist_ok=True)
            file_path = os.path.join(
                daily_dir, f"scada_report_{report_label}.csv"
            )

        pd.DataFrame(full_report).to_csv(file_path, index=False, header=False)

        self.stdout.write(
            self.style.SUCCESS(
                f"\n✅ Report Generated Successfully\n📁 {file_path}\n"
            )
        )
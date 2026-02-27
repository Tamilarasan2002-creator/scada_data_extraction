import os
import requests
import pandas as pd
from datetime import datetime
from django.core.management.base import BaseCommand
from django.db import connection
from django.conf import settings


API_URL = "http://172.16.7.118:8003/api/obs/leaplocs.php"


class Command(BaseCommand):
    help = "Generate SCADA Report (Include All LocNo Even If No Data)"

    def add_arguments(self, parser):
        parser.add_argument("--date", type=str, help="Filter by date YYYY-MM-DD")

    # -----------------------------------------------------
    # SAFE DATABASE FETCH (No Pandas Warning)
    # -----------------------------------------------------
    def fetch_data(self, query, params):
        with connection.cursor() as cursor:
            cursor.execute(query, params)
            columns = [col[0] for col in cursor.description]
            rows = cursor.fetchall()
        return pd.DataFrame(rows, columns=columns)

    # -----------------------------------------------------
    # OPTIMIZED BATCH FETCH
    # -----------------------------------------------------
    def fetch_all_data(self, report_date):
        if not report_date:
            return {}, None

        start_dt = f"{report_date} 00:00:00"
        end_dt = f"{report_date} 23:59:59"

        tables = [
            {
                "name": "inhouse_scada_data", 
                "date_col": "timestamp", 
                "asset_col": "asset_name",
                "active_power": "active_power_generation",
                "wind_speed": "windspeed_outside_nacelle",
                "temp": "temperature_outside_nacelle",
                "wind_dir": "winddirection_outside_nacelle"
            },
            {
                "name": "gtmw", 
                "date_col": "date", 
                "asset_col": "device",
                "active_power": "avg_active_power",
                "wind_speed": "avg_wind_speed",
                "temp": "avg_ambient_temperature",
                "wind_dir": "NULL" 
            },
            {
                "name": "scada_data_enercon", 
                "date_col": "date", 
                "asset_col": "asset_name",
                "active_power": "active_power_generation",
                "wind_speed": "wind_speed_outside_nacelle",
                "temp": "temperature_outside_nacelle",
                "wind_dir": "wind_direction_outside_nacelle"
            },
        ]

        # Final map: locno -> dataframe
        data_map = {}
        all_timestamps = []

        for table in tables:
            query = f"""
                SELECT TRIM(UPPER({table['asset_col']})) as locno_key,
                       {table['date_col']} as datetime,
                       {table['active_power']} as active_power_generation,
                       {table['wind_speed']} as windspeed_outside_nacelle,
                       {table['temp']} as temperature_outside_nacelle,
                       {table['wind_dir']} as winddirection_outside_nacelle
                FROM {table['name']}
                WHERE {table['date_col']} BETWEEN %s AND %s
            """
            try:
                df = self.fetch_data(query, [start_dt, end_dt])
                if not df.empty:
                    df["datetime"] = pd.to_datetime(df["datetime"])
                    all_timestamps.extend(df["datetime"].unique())
                    
                    # Group by locno to avoid repeated filtering
                    for locno, group in df.groupby("locno_key"):
                        if locno not in data_map:
                            group = group.drop(columns=["locno_key"]).sort_values("datetime").set_index("datetime")
                            # Remove duplicates if any
                            group = group[~group.index.duplicated(keep='first')]
                            data_map[locno] = group
            except Exception as e:
                self.stdout.write(f"⚠ Table {table['name']} fetch error: {str(e)}")
                continue

        # Create a full time index for the day (144 intervals of 10-min)
        master_index = pd.date_range(start=start_dt, end=end_dt, freq='10min')
        
        return data_map, master_index

    # -----------------------------------------------------
    # MAIN HANDLE
    # -----------------------------------------------------
    def handle(self, *args, **kwargs):

        report_date = kwargs.get("date")
        if not report_date:
            report_date = datetime.now().strftime('%Y-%m-%d')

        self.stdout.write(f"🚀 Initializing optimization for {report_date}...")
        
        # Batch fetch all data once
        all_data_map, global_time_index = self.fetch_all_data(report_date)

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

        # Prepare headers
        headers = {
            "lat": ["DateTime / Latitude"],
            "lon": ["Longitude"],
            "locno": ["Loc No"],
            "mac": ["Mac No"],
            "param": ["Parameters"],
            "unit": ["Units"]
        }

        machine_dataframes = []
        machine_no = 1

        structure = [
            ("windspeed_outside_nacelle", "Wind Speed at 78.5 mtr", "m/s"),
            ("winddirection_outside_nacelle", "Wind Direction at 78.5 mtr", "degree"),
            ("temperature_outside_nacelle", "Ambient Temp at 78.5 mtr", "°C"),
            ("active_power_generation", "Active_Power 78.5 mtr", ""),
        ]

        # -------------------------------------------------
        # LOOP ALL LOCNO (Pre-fetched data lookup)
        # -------------------------------------------------
        for item in api_data:
            locno = item.get("locno")
            lat = item.get("latitude")
            lon = item.get("longitude")
            
            locno_upper = locno.strip().upper()
            
            # Lookup in our pre-fetched map
            df = all_data_map.get(locno_upper, pd.DataFrame())

            # Reindex to master time index to ensure alignment
            if not df.empty:
                df = df.reindex(global_time_index)
            else:
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
                    machine_dataframes.append(pd.Series([None]*len(df), index=df.index, name=col))

            machine_no += 1

        if not machine_dataframes:
            self.stdout.write("❌ No data structures built")
            return

        # -------------------------------------------------
        # CONCAT ALL AT ONCE (Prevents fragmentation)
        # -------------------------------------------------
        self.stdout.write("📊 Finalizing report structure...")
        final_data_df = pd.concat(machine_dataframes, axis=1)
        
        # Build report rows
        full_report = [
            headers["lat"],
            headers["lon"],
            headers["locno"],
            headers["mac"],
            headers["param"],
            headers["unit"]
        ]

        # Convert data rows (values.tolist() is much faster than iterrows)
        # We need to prepend the index (timestamp) to each row
        time_stamps = global_time_index.strftime('%Y-%m-%d %H:%M:%S').tolist()
        data_values = final_data_df.values.tolist()
        
        for i, row in enumerate(data_values):
            full_report.append([time_stamps[i]] + row)

        # -------------------------------------------------
        # SAVE FILE
        # -------------------------------------------------
        timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
        folder_name = f"scada_report_{timestamp_str}"
        reports_dir = os.path.join(settings.BASE_DIR, "reports")
        os.makedirs(reports_dir, exist_ok=True)
        output_folder = os.path.join(reports_dir, folder_name)
        os.makedirs(output_folder, exist_ok=True)
        file_path = os.path.join(output_folder, f"{folder_name}.csv")

        pd.DataFrame(full_report).to_csv(file_path, index=False, header=False)

        self.stdout.write(self.style.SUCCESS(
            f"\n✅ Optimized Report Generated Successfully\n📁 {file_path}\n"
        ))
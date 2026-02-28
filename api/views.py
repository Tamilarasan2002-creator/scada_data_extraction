from rest_framework.decorators import api_view
from rest_framework.response import Response
from datetime import datetime, timedelta
from processor.models import SCADAData
import math

@api_view(['GET'])
def scada_by_date(request):

    date_str = request.GET.get('date')

    if not date_str:
        return Response({"error": "Provide date in format YYYY-MM-DD"})

    date_obj = datetime.strptime(date_str, "%Y-%m-%d").date()

    start_datetime = datetime.combine(date_obj, datetime.min.time())
    end_datetime = start_datetime + timedelta(days=1)

    queryset = SCADAData.objects.filter(
        datetime__gte=start_datetime,
        datetime__lt=end_datetime
    ).values(
        "datetime",
        "locno",
        "outdoor_temp",
        "wind_speed",
        "active_power",
        "frequency"
    )[:1000]   # limit for speed

    data = list(queryset)

    # 🔥 THIS IS WHERE NaN FIX HAPPENS
    for row in data:
        for key, value in row.items():
            if isinstance(value, float) and math.isnan(value):
                row[key] = None

    return Response(data)
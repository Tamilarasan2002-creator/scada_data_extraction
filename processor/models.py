from django.db import models

# Create your models here.

class SCADAData(models.Model):
    locno = models.CharField(max_length=10)
    datetime = models.DateTimeField()
    outdoor_temp = models.FloatField()
    wind_speed = models.FloatField()
    nacelle_pos = models.FloatField()
    active_power = models.FloatField()
    frequency = models.FloatField()

    def __str__(self):
        return self.locno

    class Meta:
        db_table = "scada_data"
        indexes = [
            models.Index(fields=["datetime","locno"]),
        ]
        constraints = [
            models.UniqueConstraint(fields=["datetime", "locno"], name="unique_datetime_locno")
        ]


class ScadaDataEnercon(models.Model):
    date = models.DateTimeField()
    asset_name = models.CharField(max_length=100)

    active_power_generation = models.FloatField(null=True, blank=True)
    wind_direction_outside_nacelle = models.FloatField(null=True, blank=True)
    wind_speed_outside_nacelle = models.FloatField(null=True, blank=True)
    temperature_outside_nacelle = models.FloatField(null=True, blank=True)

    class Meta:
        db_table = "scada_data_enercon"
        managed = False   # IMPORTANT (table already exists)




    
class GTMW(models.Model):
    device = models.CharField(max_length=100)
    date = models.DateTimeField()
    quality = models.CharField(max_length=50, null=True, blank=True)  # ✅ NEW
    misalignment_percent = models.FloatField(null=True, blank=True)
    avg_active_power = models.FloatField(null=True, blank=True)
    avg_ambient_temperature = models.FloatField(null=True, blank=True)
    avg_wind_speed = models.FloatField(null=True, blank=True)

    class Meta:
        db_table = "gtmw"
        unique_together = ("device", "date")
        indexes = [
            models.Index(fields=["device", "date"]),
        ]

    def __str__(self):
        return f"{self.device} - {self.date}"





class InhouseSCADAData(models.Model):
    timestamp = models.DateTimeField()
    asset_name = models.CharField(max_length=50)

    active_power_generation = models.FloatField(null=True, blank=True)
    windspeed_outside_nacelle = models.FloatField(null=True, blank=True)
    temperature_outside_nacelle = models.FloatField(null=True, blank=True)
    winddirection_outside_nacelle = models.FloatField(null=True, blank=True)

    class Meta:
        db_table = "inhouse_scada_data"
        unique_together = ("timestamp", "asset_name")
        indexes = [
            models.Index(fields=["timestamp"]),
            models.Index(fields=["asset_name"]),
        ]

    def __str__(self):
        return f"{self.asset_name} - {self.timestamp}"
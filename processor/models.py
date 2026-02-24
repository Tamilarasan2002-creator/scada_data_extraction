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
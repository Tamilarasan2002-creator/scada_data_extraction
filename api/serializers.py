import math
from rest_framework import serializers
from processor.models import SCADAData

class SCADADataSerializer(serializers.ModelSerializer):

    class Meta:
        model = SCADAData
        fields = "__all__"

    def to_representation(self, instance):
        data = super().to_representation(instance)

        for key, value in data.items():
            if isinstance(value, float) and math.isnan(value):
                data[key] = None

        return data
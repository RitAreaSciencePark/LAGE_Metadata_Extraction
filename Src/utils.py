# SPDX-FileCopyrightText: 2026  Lesly TSOPTIO FOUGANG, Valerio PIOMPONI,
# Ornella AFFINITO, Laboratory of Data Engineering, RIT, Area Science Park.
# SPDX-License-Identifier: MIT

import json
import math


class NaNSafeEncoder(json.JSONEncoder):
    """
    Custom JSON encoder that recursively converts float NaN and
    Infinity values to null before serialisation.
    Handles pandas-derived data containing missing values (NaN)
    produced when reading CSV files with empty fields.
    """
    def default(self, obj):
        return super().default(obj)

    def iterencode(self, obj, _one_shot=False):
        return super().iterencode(self._clean(obj), _one_shot)

    def _clean(self, obj):
        if isinstance(obj, dict):
            return {k: self._clean(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._clean(v) for v in obj]
        elif isinstance(obj, float) and (
                math.isnan(obj) or math.isinf(obj)):
            return None
        return obj


def safe_dump(data, file_obj, indent=4):
    """
    Drop-in replacement for json.dump() that safely handles
    float NaN and Infinity values by converting them to null.

    Usage — replace every:
        json.dump(data, f, indent=4)
    with:
        safe_dump(data, f, indent=4)
    """
    json.dump(data, file_obj, indent=indent, cls=NaNSafeEncoder)
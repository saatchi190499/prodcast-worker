"Utility functions for project"
# src/utils/utils.py

def handle_large_values(value):
    try:
        float_value = float(value)
        if float_value > 1e+10:
            return 0
        return float_value
    except ValueError:
        return 0

 

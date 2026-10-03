"""Safe public errors; engine errors use the same fixed projection."""
def safe_error(error, stage="data"):
    return f"Error: external_operation_failed ({stage})"

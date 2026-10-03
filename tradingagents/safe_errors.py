"""Project-safe projection at the first exception-to-text boundary."""

def safe_error(error, stage="data"):
    # Never include exception text, URLs, local paths or response bodies.
    return f"Error: external_operation_failed ({stage})"

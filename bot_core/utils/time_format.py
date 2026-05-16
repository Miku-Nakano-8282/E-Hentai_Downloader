def format_time(seconds):
    """Convert seconds into a clean time string."""
    if seconds is None or seconds < 0:
        return "calculating..."

    seconds = int(seconds)
    if seconds < 60:
        return f"{seconds}s"

    minutes, sec = divmod(seconds, 60)
    if minutes < 60:
        return f"{minutes}m {sec}s"

    hours, minutes = divmod(minutes, 60)
    if hours < 24:
        return f"{hours}h {minutes}m {sec}s"

    days, hours = divmod(hours, 24)
    return f"{days}d {hours}h {minutes}m"

import time

def monitor_queue_and_stats(queue, logger, stats_manager):
    """Periodically log the queue size and stats."""
    while True:
        # Log queue size
        logger.info(f"Current queue size: {queue.qsize()}")

        # Log monthly and lifetime stats
        month_key = stats_manager.get_month_key()
        monthly_stats = stats_manager.stats.get(month_key, {
            "files_converted": 0,
            "failed_encodings": 0,
            "total_space_saved_bytes": 0,
            "total_input_size_bytes": 0,
            "total_output_size_bytes": 0,
            "total_encoding_time_seconds": 0
        })
        lifetime_stats = stats_manager.get_lifetime_stats()

        logger.info(stats_manager.format_stats(monthly_stats, f"Monthly ({month_key})"))
        logger.info(stats_manager.format_stats(lifetime_stats, "Lifetime"))

        time.sleep(60)
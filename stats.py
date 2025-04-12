import json
import os
from datetime import datetime

class StatsManager:
    def __init__(self, stats_file, logger):
        self.stats_file = stats_file
        self.logger = logger
        self.stats = self.load_stats()

    def load_stats(self):
        """Load stats from stats.json, or initialize if the file doesn't exist."""
        if not os.path.exists(self.stats_file):
            self.logger.info(f"Stats file {self.stats_file} not found, initializing new stats")
            return {}

        try:
            with open(self.stats_file, 'r') as f:
                return json.load(f)
        except Exception as e:
            self.logger.error(f"Error loading stats from {self.stats_file}: {str(e)}")
            return {}

    def save_stats(self):
        """Save stats to stats.json."""
        try:
            with open(self.stats_file, 'w') as f:
                json.dump(self.stats, f, indent=4)
            self.logger.debug(f"Updated stats saved to {self.stats_file}")
        except Exception as e:
            self.logger.error(f"Error saving stats to {self.stats_file}: {str(e)}")

    def get_month_key(self):
        """Get the current month key in YYYY-MM format."""
        return datetime.now().strftime("%Y-%m")

    def update_stats(self, input_size, output_size, encoding_time, success=True):
        """Update stats after an encoding attempt."""
        month_key = self.get_month_key()
        
        # Initialize stats for the month if not present
        if month_key not in self.stats:
            self.stats[month_key] = {
                "files_converted": 0,
                "failed_encodings": 0,
                "total_space_saved_bytes": 0,
                "total_input_size_bytes": 0,
                "total_output_size_bytes": 0,
                "total_encoding_time_seconds": 0
            }

        month_stats = self.stats[month_key]
        
        # Update metrics
        if success:
            month_stats["files_converted"] += 1
            space_saved = input_size - output_size if output_size > 0 else 0
            month_stats["total_space_saved_bytes"] += space_saved
            month_stats["total_input_size_bytes"] += input_size
            month_stats["total_output_size_bytes"] += output_size
            month_stats["total_encoding_time_seconds"] += encoding_time
        else:
            month_stats["failed_encodings"] += 1

        self.save_stats()

    def get_lifetime_stats(self):
        """Calculate lifetime stats across all months."""
        lifetime_stats = {
            "files_converted": 0,
            "failed_encodings": 0,
            "total_space_saved_bytes": 0,
            "total_input_size_bytes": 0,
            "total_output_size_bytes": 0,
            "total_encoding_time_seconds": 0
        }

        for month_stats in self.stats.values():
            for key in lifetime_stats:
                lifetime_stats[key] += month_stats[key]

        return lifetime_stats

    def format_stats(self, stats, label):
        """Format stats for logging."""
        return (
            f"{label} Stats:\n"
            f"  Files Converted: {stats['files_converted']}\n"
            f"  Failed Encodings: {stats['failed_encodings']}\n"
            f"  Total Space Saved: {stats['total_space_saved_bytes'] / (1024 * 1024):.2f} MB\n"
            f"  Total Input Size: {stats['total_input_size_bytes'] / (1024 * 1024):.2f} MB\n"
            f"  Total Output Size: {stats['total_output_size_bytes'] / (1024 * 1024):.2f} MB\n"
            f"  Total Encoding Time: {stats['total_encoding_time_seconds'] / 3600:.2f} hours"
        )
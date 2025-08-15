from typing import Dict, Any
import yaml
import os
import logging
import sys
from .weather_model import WeatherModel
from .ensemble import Ensemble
from . import database

def setup_logging(config: Dict[str, Any]) -> None:
    """Set up logging based on the configuration."""
    logging_config = config.get('logging', {})
    level = logging_config.get('level', 'INFO').upper()
    log_file = logging_config.get('file')
    log_to_console = logging_config.get('console', True)

    handlers = []
    if log_file:
        handlers.append(logging.FileHandler(log_file, mode='a'))
    if log_to_console:
        handlers.append(logging.StreamHandler(sys.stdout))

    if not handlers:
        # Default to console if no handler is configured
        handlers.append(logging.StreamHandler(sys.stdout))

    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=handlers
    )

def load_config(config_path: str) -> Dict[str, Any]:
    """Load configuration from a YAML archive"""
    try:
        with open(config_path, 'r', encoding='utf-8') as file:
            config = yaml.safe_load(file)
            return config if isinstance(config, dict) else {}
    except FileNotFoundError:
        logging.error(f"Error: File {config_path} not found")
        return {}
    except yaml.YAMLError as e:
        logging.error(f"Error reading YAML file: {e}")
        return {}

def main():
    """
    Main function to orchestrate the weather model data workflow.
    """

    # 1. Load config
    config = load_config('resources/default_config.yaml')
    if not config:
        return

    # Setup logging
    setup_logging(config)

    # 2. Initialize database
    database.create_tables()

    # 3. Purge old data
    retention_days = config.get('database', {}).get('retention_days')
    if retention_days:
        database.purge_old_runs(retention_days)

    model_used = config.get('models_used', [])

    # 4. Create weather models instances
    all_models_attempted = [WeatherModel(name, config) for name in model_used]

    # 5. Filter valid and complete models    
    models = [model for model in all_models_attempted if model.is_valid and model.data]
 
    if any(model.is_new for model in models):
        logging.info("New models downloaded")
    else:
        logging.info("No new model runs")

    # 6. Save output

    new_models = [model for model in models if model.is_new]

    output_dir = 'output'
    os.makedirs(output_dir, exist_ok=True)

    if new_models:
        for new_model in new_models:
            # Export statistics to CSV
            new_model.export_statistics_to_csv(output_dir, config)

    # 7 Create and export ensemble
    
        logging.info("--- Creating and exporting ensemble ---")
        ensemble = Ensemble(models, config)
        ensemble.to_csv(output_dir, config)
        ensemble.save_to_db()

        # 8. Create and save HTML table
        logging.info("--- Creating and exporting HTML table ---")
        html_table = ensemble.to_html_table(config)
        html_filepath = os.path.join(output_dir, "ensemble_forecast.html")
        try:
            with open(html_filepath, "w", encoding="utf-8") as f:
                f.write(html_table)
            logging.info(f"Successfully exported HTML table to {html_filepath}")
        except IOError as e:
            logging.error(f"Error exporting HTML table to {html_filepath}: {e}")

if __name__ == "__main__":
    main()
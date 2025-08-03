from typing import Dict, Any
import yaml
import os
from datetime import datetime, timedelta
from .weather_model import WeatherModel
from . import database

def load_config(config_path: str) -> Dict[str, Any]:
    """Load configuration from a YAML archive"""
    try:
        with open(config_path, 'r', encoding='utf-8') as file:
            config = yaml.safe_load(file)
            return config if isinstance(config, dict) else {}
    except FileNotFoundError:
        print(f"Error: File {config_path} not found")
        return {}
    except yaml.YAMLError as e:
        print(f"Error reading YAML file: {e}")
        return {}

def main():
    """
    Main function to orchestrate the weather model data workflow.
    """

    # 1. Load config
    config = load_config('resources/default_config.yaml')
    if not config:
        return

    # Initialize database
    database.create_tables()

    # Purge old data
    retention_days = config.get('database', {}).get('retention_days')
    if retention_days:
        database.purge_old_runs(retention_days)

    model_used = config.get('models_used', [])

    # 2. Create weather models instances
    models = [WeatherModel(name, config) for name in model_used]

    # 3. Identify models with new runs
    new_models = [model for model in models if model.check_if_new()]

    # 4. Process only the models that have new runs
    if not new_models:
        print("No new model runs found for any model. Exiting.")
        return

    print(f"Found new runs for the following models: {[model.name for model in new_models]}")

    # Create output directory if it doesn't exist
    output_dir = 'output'
    os.makedirs(output_dir, exist_ok=True)

    for model in new_models:
        print(f"\n--- Processing model: {model.name} ---")
        model.print_metadata()

        if model.metadata:
            availability_time = model.metadata.get('last_run_availability_time')
            if availability_time and isinstance(availability_time, datetime):
                if datetime.now() - availability_time < timedelta(minutes=10):
                    print(f"Last run for {model.name} was available less than 10 minutes ago.")
                    print("To ensure data integrity, please wait a few more minutes before downloading.")
                    continue # Skip to the next model in the new_models list

        model.retrieve_data(config)
        model.calculate_statistics()

        # Save data to database
        conn = database.get_db_connection()
        cursor = conn.cursor()
        cursor.execute("INSERT INTO forecast_runs (model_name, run_timestamp) VALUES (?, ?)",
                       (model.name, model.last_run_time))
        run_id = cursor.lastrowid

        # Clean up any previous data for this run_id to prevent duplicates
        cursor.execute("DELETE FROM raw_forecast_data WHERE run_id = ?", (run_id,))
        cursor.execute("DELETE FROM statistical_forecasts WHERE run_id = ?", (run_id,))

        conn.commit()
        conn.close()

        if run_id:
            model._save_raw_data_to_db(run_id)
            model._save_statistics_to_db(run_id)
            # model.print_statistics()
            model.export_statistics_to_csv(output_dir, config)

if __name__ == "__main__":
    main()


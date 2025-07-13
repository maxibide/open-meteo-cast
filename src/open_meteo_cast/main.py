from typing import Dict, Any
import yaml
import os
from datetime import datetime, timedelta
from .weather_model import WeatherModel

def load_config(config_path: str) -> Dict[str, Any]:
    """Load configuration from a YAML archive"""
    try:
        with open(config_path, 'r', encoding='utf-8') as file:
            return yaml.safe_load(file)  # type: ignore[no-any-return]
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
        model.print_data()
        model.calculate_statistics()
        model.print_statistics()
        model.export_statistics_to_csv(output_dir, config)


if __name__ == "__main__":
    main()


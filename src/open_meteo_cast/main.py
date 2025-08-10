from typing import Dict, Any
import yaml
import os
from .weather_model import WeatherModel
from .ensemble import Ensemble
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
        print("New models downloaded")
    else:
        print("No new model runs")

    # 6. Save output

    new_models = [model for model in models if model.is_new]

    output_dir = 'output'
    os.makedirs(output_dir, exist_ok=True)

    if new_models:
        for new_model in new_models:
            # Export statistics to CSV
            new_model.export_statistics_to_csv(output_dir, config)

    # 7 Create and export ensemble
    
        print("\n--- Creating and exporting ensemble ---")
        ensemble = Ensemble(models, config)
        ensemble.to_csv(output_dir, config)

if __name__ == "__main__":
    main()


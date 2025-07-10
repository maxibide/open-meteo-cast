from typing import Dict, Any
import yaml
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

    model_used = ['gfs025']

    # 2. Create wheather models instances
    models = [WeatherModel(name, config) for name in model_used]

    # 3. Check which models have new runs
    model_is_new_flags = [model.check_if_new() for model in models]

    if not any(model_is_new_flags):
        print("No new model runs found for any model. Exiting.")
    else:
        print("New model runs found")

    # 4. Print metadata
    for model in models:
        model.print_metadata()
        model.retrieve_data(config)
        model.print_data()


if __name__ == "__main__":
    main()
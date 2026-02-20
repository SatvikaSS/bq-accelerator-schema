import sys
import os
# Ensure project root is in path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
 
from app.execution.config_executor import ConfigExecutor
 
 
def main():
    if len(sys.argv) != 2:
        print("Usage: python run_config.py <config.yaml>")
        sys.exit(1)
 
    config_path = sys.argv[1]
    executor = ConfigExecutor(config_path)
    result = executor.execute()
 
    print("\n=== Execution Completed ===")
    print(f"Entity: {result.get('entity')}")
    print(f"Version: {result.get('version')}")
    print(f"Decision: {result.get('decision')}")
 
 
if __name__ == "__main__":
    main()
 
 
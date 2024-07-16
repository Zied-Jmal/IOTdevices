
# main.py
import multiprocessing
import uvicorn
import sys
import os

# Ensure the script's directory is in the system path
#sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

def run_server(port):
    from src.api.config_api.config_api import app  # Replace 'your_fastapi_app' with the name of your FastAPI script without the .py extension
    uvicorn.run(app, host="0.0.0.0", port=port)

if __name__ == "__main__":
    port1 = 8001
    port2 = 8002

    # Create two processes to run two instances of the FastAPI app on different ports
    p1 = multiprocessing.Process(target=run_server, args=(port1,))
    p2 = multiprocessing.Process(target=run_server, args=(port2,))

    p1.start()
    p2.start()

    p1.join()
    p2.join()

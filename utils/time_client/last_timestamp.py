import json
import os
from datetime import datetime,timezone

def load_state():
    if not os.path.exists("state.json"):
        return {}
    with open("state.json", "r") as f:
        return json.load(f)
    
def get_last_timestamp(source):
    state = load_state()
    return state.get(source)

def update_state(source, new_timestamp):
    state = load_state()
    state[source] = new_timestamp

    with open("state.json", "w") as f:
        json.dump(state, f, indent=2)

def get_current_utc_time():
    current_utc_time = datetime.now(timezone.utc).isoformat()
    return current_utc_time.split(".")[0]

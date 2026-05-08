import os
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

def get_supabase():
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY")
    return create_client(url, key)

def load_snapshots_to_supabase(snapshots: list):
    if not snapshots:
        print("No snapshots to load")
        return
    supabase = get_supabase()
    # batch insert in chunks of 500
    chunk_size = 500
    for i in range(0, len(snapshots), chunk_size):
        chunk = snapshots[i:i + chunk_size]
        supabase.table("game_snapshots").insert(chunk).execute()
        print(f"Inserted {i + len(chunk)}/{len(snapshots)} snapshots")

def load_shots_to_supabase(shots: list):
    if not shots:
        return
    supabase = get_supabase()
    chunk_size = 500
    for i in range(0, len(shots), chunk_size):
        chunk = shots[i:i + chunk_size]
        supabase.table("shots").insert(chunk).execute()
        print(f"Inserted {i + len(chunk)}/{len(shots)} shots")
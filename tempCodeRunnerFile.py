print("All tables available in sqlite db: ")
cursor_obj.execute("SELECT name FROM sqlite_master WHERE type='table';")
print(cursor_obj.fetchall())
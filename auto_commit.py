import os
import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import subprocess

class Watcher:
    DIRECTORY_TO_WATCH = "C:/Users/New/Desktop/github"

    def __init__(self):
        self.observer = Observer()

    def run(self):
        event_handler = Handler()
        self.observer.schedule(event_handler, self.DIRECTORY_TO_WATCH, recursive=True)
        self.observer.start()
        try:
            while True:
                time.sleep(5)
        except:
            self.observer.stop()
            print("Observer Stopped")

        self.observer.join()

class Handler(FileSystemEventHandler):
    def on_any_event(self, event):
        if event.is_directory:
            return None
        elif event.event_type in ['modified', 'created']:
            print(f"Detected change in {event.src_path}")
            try:
                subprocess.run(["git", "add", "."], cwd=Watcher.DIRECTORY_TO_WATCH, check=True)
                subprocess.run(["git", "commit", "-m", "Auto commit"], cwd=Watcher.DIRECTORY_TO_WATCH, check=True)
                subprocess.run(["git", "push", "origin", "main"], cwd=Watcher.DIRECTORY_TO_WATCH, check=True)
                print("Changes committed and pushed to GitHub")
            except subprocess.CalledProcessError as e:
                print(f"Error during git operations: {e}")

if __name__ == "__main__":
    w = Watcher()
    w.run()
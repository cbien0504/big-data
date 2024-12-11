import subprocess
files = ['users.py', 'tweets.py', 'projects.py']
processes = []
for file in files:
    process = subprocess.Popen(['python3', f"batch-processing/{file}"])
    processes.append(process)
for process in processes:
    process.communicate()

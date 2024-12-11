import subprocess
files = ['users_streaming.py', 'tweets_streaming.py', 'projects_streaming.py']
processes = []
for file in files:
    process = subprocess.Popen(['python3', f"spark-streaming/{file}"])
    processes.append(process)
for process in processes:
    process.communicate()

import glob, os, re, sys

dag, task = sys.argv[1], sys.argv[2]
files = sorted(glob.glob(f"/opt/airflow/logs/dag_id={dag}/run_id=*/task_id={task}/attempt=*.log"), key=os.path.getmtime)
if not files:
    sys.exit("aucun journal trouve")
for f in files[-2:]:
    print("=====", f)
    lines = open(f, encoding="utf-8", errors="replace").read().splitlines()
    keys = re.compile(r"progression|ERROR|Error|Traceback|429|Timeout|Exception|Read timed out")
    hits = [l for l in lines if keys.search(l)]
    print(f"{len(lines)} lignes, {len(hits)} lignes utiles ; dernieres :")
    for l in hits[-12:]:
        print(l[:260])
    print("--- fin du journal :")
    for l in lines[-5:]:
        print(l[:260])

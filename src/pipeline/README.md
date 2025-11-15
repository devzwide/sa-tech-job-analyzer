├── DataPipeline/          <-- RENAMED
│   ├── __init__.py        <-- Makes this a Python package
│   ├── run_pipeline.py    <-- NEW: The main entrypoint
│   ├── database.py        <-- MOVED: All DB logic
│   ├── scraper.py         <-- MOVED: All scraping logic
│   ├── transforms.py      <-- MOVED: All cleaning logic
│   ├── logging_config.py  <-- RENAMED: (from monitor.py)
│   ├── requirements.txt
│   └── Dockerfile         <-- For containerizing the pipeline
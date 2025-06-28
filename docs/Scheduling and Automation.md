## Scheduling & Automation

This is currently scheduled using Windows Task Scheduler. It currently runs two separate tasks on a daily basis, with minmal runtime and logging.

It would be an excellent candidate for Airflow integration in a more scalable production setting.

---

### Full Scrape of Design Folder

- **Run Method**: Task Scheduler
- **Frequency**: Daily @ 8:30am
- **Run Time**: ~10 seconds
- **Logs**: `/logs/app.log` (rotating file logger)
    - Logs capture ETL run results, warnings for skipped files, and error traces if failures occur.

---

### Staging Folder Processing
- **Run Method**: Task Scheduler
- **Frequency**: Daily @ 9:00am
- **Run Time**: ~10 seconds
- **Logs**: `/logs/app.log` (rotating file logger)
    - Logs capture ETL run results, warnings for skipped files, and error traces if failures occur.

---
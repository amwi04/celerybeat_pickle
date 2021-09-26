# celerybeat_pickle
Using pickle for scheduling in beats.

pickle is much more convienent that shelves.

```
celery -A territoryhub.celery_app beat --max-interval 10 --scheduler celerybeat_pickle.persistance:PersistentSchedulerOpen
```


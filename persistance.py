from celery.beat import Scheduler
from celery.beat import __version__,platforms,signals
from celery.exceptions import reraise
from celery.schedules import crontab, maybe_schedule
from celery.utils.imports import load_extension_class_names, symbol_by_name
from celery.utils.log import get_logger, iter_open_logger_fds
from celery.utils.time import humanize_seconds, maybe_make_aware
from collections import namedtuple
import errno,os,pickle,io

event_t = namedtuple('event_t', ('time', 'priority', 'entry'))

logger = get_logger(__name__)

debug, info, error, warning = (logger.debug, logger.info,
                               logger.error, logger.warning)

class PersistentSchedulerOpen(Scheduler):
    """Scheduler backed by :mod:`open` database."""

    persistence = io
    known_suffixes = ('', '.db', '.dat', '.bak', '.dir')

    _store = None

    def __init__(self, *args, **kwargs):
        self.schedule_filename = kwargs.get('schedule_filename')
        Scheduler.__init__(self, *args, **kwargs)

    def _remove_db(self):
        for suffix in self.known_suffixes:
            with platforms.ignore_errno(errno.ENOENT):
                os.remove(self.schedule_filename + suffix)

    def _open_schedule(self):
        try:
            a = self.persistence.open(self.schedule_filename, mode='rb')
            self.data = pickle.load(a)
            a.close()
        except Exception as e:
            print(e)
            self.data = {}
        return self.data

    def _destroy_open_corrupted_schedule(self, exc):
        error('Removing corrupted schedule file %r: %r',
              self.schedule_filename, exc, exc_info=True)
        self._remove_db()
        return self._open_schedule()


    def setup_schedule(self):
        try:
            self._store = self._open_schedule()
            # In some cases there may be different errors from a storage
            # backend for corrupted files.  Example - DBPageNotFoundError
            # exception from bsddb.  In such case the file will be
            # successfully opened but the error will be raised on first key
            # retrieving.
            self._store.keys()
        except Exception as exc:  # pylint: disable=broad-except
            self._store = self._destroy_open_corrupted_schedule(exc)

        self._create_schedule()

        tz = self.app.conf.timezone
        stored_tz = self._store.get('tz')
        if stored_tz is not None and stored_tz != tz:
            warning('Reset: Timezone changed from %r to %r', stored_tz, tz)
            self._store.clear()   # Timezone changed, reset db!
        utc = self.app.conf.enable_utc
        stored_utc = self._store.get('utc_enabled')
        if stored_utc is not None and stored_utc != utc:
            choices = {True: 'enabled', False: 'disabled'}
            warning('Reset: UTC changed from %s to %s',
                    choices[stored_utc], choices[utc])
            self._store.clear()   # UTC setting changed, reset db!
        entries = self._store.setdefault('entries', {})
        self.merge_inplace(self.app.conf.beat_schedule)
        self.install_default_entries(self.schedule)
        self._store.update({
            '__version__': __version__,
            'tz': tz,
            'utc_enabled': utc,
        })
        self.sync()
        debug('Current schedule:\n' + '\n'.join(
            repr(entry) for entry in entries.values()))


    def _create_schedule(self):
        for _ in (1, 2):
            try:
                self._store['entries']
            except KeyError:
                # new schedule db
                try:
                    self._store['entries'] = {}
                except KeyError as exc:
                    self._store = self._destroy_open_corrupted_schedule(exc)
                    continue
            else:
                if '__version__' not in self._store:
                    warning('DB Reset: Account for new __version__ field')
                    self._store.clear()   # remove schedule at 2.2.2 upgrade.
                elif 'tz' not in self._store:
                    warning('DB Reset: Account for new tz field')
                    self._store.clear()   # remove schedule at 3.0.8 upgrade
                elif 'utc_enabled' not in self._store:
                    warning('DB Reset: Account for new utc_enabled field')
                    self._store.clear()   # remove schedule at 3.0.9 upgrade
            break

    def get_schedule(self):
        return self._store['entries']

    def set_schedule(self, schedule):
        self._store['entries'] = schedule

    schedule = property(get_schedule, set_schedule)


    def sync(self):
        if self._store is not None:
            with open(self.schedule_filename, 'wb') as f:
                pickle.dump(self._store, f, pickle.HIGHEST_PROTOCOL)

    def close(self):
        self.sync()

    @property
    def info(self):
        return f'    . db -> {self.schedule_filename}'
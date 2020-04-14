#!/usr/bin/python
# -*- coding: utf-8 -*-
# DATE: 2020/4/02
# Author: clarkmonkey@163.com
import os
import threading
from time import time as ct, sleep
from sqlite3 import connect, Cursor, Connection, OperationalError
from typing import *
from contextlib import contextmanager

PRAGMA_SETTINGS = {
    'auto_vacuum': 1,  # FULL
    'cache_size': 1 << 13,  # 8,192 pages
    'journal_mode': 'wal',
    # Query or change the value of the sqlite3_limit(db,SQLITE_LIMIT_WORKER_THREADS,...) limit for the current database connection. This limit sets an upper bound on the number of auxiliary threads that a prepared statement is allowed to launch to assist with a query. The default limit is 0 unless it is changed using the SQLITE_DEFAULT_WORKER_THREADS compile-time option. When the limit is zero, that means no auxiliary threads will be launched.
    #
    # This pragma is a thin wrapper around the sqlite3_limit(db,SQLITE_LIMIT_WORKER_THREADS,...) interface.
    'threads': 1,
    # Query or change the setting of the "temp_store" parameter. When temp_store is DEFAULT (0), the compile-time C preprocessor macro SQLITE_TEMP_STORE is used to determine where temporary tables and indices are stored. When temp_store is MEMORY (2) temporary tables and indices are kept in as if they were pure in-memory databases memory. When temp_store is FILE (1) temporary tables and indices are stored in a file. The temp_store_directory pragma can be used to specify the directory containing temporary files when FILE is specified. When the temp_store setting is changed, all existing temporary tables, indices, triggers, and views are immediately deleted.
    #
    # It is possible for the library compile-time C preprocessor symbol SQLITE_TEMP_STORE to override this pragma setting. The following table summarizes the interaction of the SQLITE_TEMP_STORE preprocessor macro and the temp_store pragma:
    'temp_store': 2,  # 0 | DEFAULT | 1 | FILE | 2 | MEMORY;
    'mmap_size': 1 << 27,  # 128MB
    # Query or change the setting of the "synchronous" flag. The first (query) form will return the synchronous setting as an integer. The second form changes the synchronous setting. The meanings of the various synchronous settings are as follows:
    #
    # EXTRA (3)
    # EXTRA synchronous is like FULL with the addition that the directory containing a rollback journal is synced after that journal is unlinked to commit a transaction in DELETE mode. EXTRA provides additional durability if the commit is followed closely by a power loss.
    # FULL (2)
    # When synchronous is FULL (2), the SQLite database engine will use the xSync method of the VFS to ensure that all content is safely written to the disk surface prior to continuing. This ensures that an operating system crash or power failure will not corrupt the database. FULL synchronous is very safe, but it is also slower. FULL is the most commonly used synchronous setting when not in WAL mode.
    # NORMAL (1)
    # When synchronous is NORMAL (1), the SQLite database engine will still sync at the most critical moments, but less often than in FULL mode. There is a very small (though non-zero) chance that a power failure at just the wrong time could corrupt the database in journal_mode=DELETE on an older filesystem. WAL mode is safe from corruption with synchronous=NORMAL, and probably DELETE mode is safe too on modern filesystems. WAL mode is always consistent with synchronous=NORMAL, but WAL mode does lose durability. A transaction committed in WAL mode with synchronous=NORMAL might roll back following a power loss or system crash. Transactions are durable across application crashes regardless of the synchronous setting or journal mode. The synchronous=NORMAL setting is a good choice for most applications running in WAL mode.
    # OFF (0)
    # With synchronous OFF (0), SQLite continues without syncing as soon as it has handed data off to the operating system. If the application running SQLite crashes, the data will be safe, but the database might become corrupted if the operating system crashes or the computer loses power before that data has been written to the disk surface. On the other hand, commits can be orders of magnitude faster with synchronous OFF.
    # In WAL mode when synchronous is NORMAL (1), the WAL file is synchronized before each checkpoint and the database file is synchronized after each completed checkpoint and the WAL file header is synchronized when a WAL file begins to be reused after a checkpoint, but no sync operations occur during most transactions. With synchronous=FULL in WAL mode, an additional sync operation of the WAL file happens after each transaction commit. The extra WAL sync following each transaction help ensure that transactions are durable across a power loss. Transactions are consistent with or without the extra syncs provided by synchronous=FULL. If durability is not a concern, then synchronous=NORMAL is normally all one needs in WAL mode.
    #
    # The TEMP schema always has synchronous=OFF since the content of of TEMP is ephemeral and is not expected to survive a power outage. Attempts to change the synchronous setting for TEMP are silently ignored.
    #
    # See also the fullfsync and checkpoint_fullfsync pragmas.
    'synchronous': 1,  # NORMAL

    # NORMAL
    # EXCLUSIVE
    # This pragma sets or queries the database connection locking-mode. The locking-mode is either NORMAL or EXCLUSIVE.
    #
    # In NORMAL locking-mode (the default unless overridden at compile-time using SQLITE_DEFAULT_LOCKING_MODE), a database connection unlocks the database file at the conclusion of each read or write transaction. When the locking-mode is set to EXCLUSIVE, the database connection never releases file-locks. The first time the database is read in EXCLUSIVE mode, a shared lock is obtained and held. The first time the database is written, an exclusive lock is obtained and held.
    #
    # Database locks obtained by a connection in EXCLUSIVE mode may be released either by closing the database connection, or by setting the locking-mode back to NORMAL using this pragma and then accessing the database file (for read or write). Simply setting the locking-mode to NORMAL is not enough - locks are not released until the next time the database file is accessed.
    #
    # There are three reasons to set the locking-mode to EXCLUSIVE.
    #
    # The application wants to prevent other processes from accessing the database file.
    # The number of system calls for filesystem operations is reduced, possibly resulting in a small performance increase.
    # WAL databases can be accessed in EXCLUSIVE mode without the use of shared memory. (Additional information)
    # When the locking_mode pragma specifies a particular database, for example:
    #
    # PRAGMA main.locking_mode=EXCLUSIVE;
    # Then the locking mode applies only to the named database. If no database name qualifier precedes the "locking_mode" keyword then the locking mode is applied to all databases, including any new databases added by subsequent ATTACH commands.
    #
    # The "temp" database (in which TEMP tables and indices are stored) and in-memory databases always uses exclusive locking mode. The locking mode of temp and in-memory databases cannot be changed. All other databases use the normal locking mode by default and are affected by this pragma.
    #
    # If the locking mode is EXCLUSIVE when first entering WAL journal mode, then the locking mode cannot be changed to NORMAL until after exiting WAL journal mode. If the locking mode is NORMAL when first entering WAL journal mode, then the locking mode can be changed between NORMAL and EXCLUSIVE and back again at any time and without needing to exit WAL journal mode.
    'locking_mode': 'EXCLUSIVE',


    # Query, set, or clear READ UNCOMMITTED isolation. The default isolation level for SQLite is SERIALIZABLE. Any process or thread can select READ UNCOMMITTED isolation, but SERIALIZABLE will still be used except between connections that share a common page and schema cache. Cache sharing is enabled using the sqlite3_enable_shared_cache() API. Cache sharing is disabled by default.
    #
    # See SQLite Shared-Cache Mode for additional information.
    # 程序不大可能进行并行操作，并且少量的不安全操作完全可以被容忍。
    # 'read_uncommitted': True,

    # sqlite3.DatabaseError no more rows available
}


class SQLiteSession:

    def __init__(self, db: Any, **settings) -> None:

        self.settings: Dict[str, Any] = settings
        self.db: str = db
        self._timeout: int = self.settings.pop('timeout', 60)   # default 60s
        self._isolation: str = self.settings.pop('isolation', None)   # default None
        self._local = threading.local()
        self._session: Optional[Connection] = None

    def close(self):
        """Close database connection."""
        # self._session = getattr(self._local, 'session', None)

        if self._session is None:
            return

        self._session.close()
        self._session = None

    def open(self) -> NoReturn:
        self._session = connect(
            self.db,
            timeout=self._timeout,
            isolation_level=self._isolation,
            check_same_thread=False
        )

        # Some SQLite pragmas work on a per-connection basis so
        # query the Settings table and reset the pragmas. The
        # Settings table may not exist so catch and ignore the
        # OperationalError that may occur.

        # Avoid setting pragma values that are already set. Pragma settings like
        # auto_vacuum and journal_mode can take a long time or may not work after
        # tables have been created.

        statement: str = 'PRAGMA %s=%s'
        script: str = ';'.join(statement % pragma for pragma in self.settings.items())
        self._retry(self._session.executescript, script)

    @property
    def session(self) -> Connection:
        # Check process ID to support process forking. If the process
        # ID changes, close the connection and update the process ID.

        local_pid: int = getattr(self._local, 'pid', None)
        pid: int = os.getpid()

        if local_pid != pid:
            self.close()
            self._local.pid = pid

        if self._session is None:
            self.open()

        return self._session

        # session: Connection = getattr(self._local, 'session', None)
        #
        # if session is None:
        #     session = self._local.session = connect(
        #         self.db,
        #         timeout=self._timeout,
        #         isolation_level=self._isolation,
        #     )
        #
        #     # Some SQLite pragmas work on a per-connection basis so
        #     # query the Settings table and reset the pragmas. The
        #     # Settings table may not exist so catch and ignore the
        #     # OperationalError that may occur.
        #     self.config_pragma(self.settings, session)

        # return session

    @property
    def sql(self) -> Callable[[Any, ...], Cursor]:
        return self.session.execute

    def retry_sql(self, statement: str, *args, **kwargs) -> Cursor:

        # 2018-11-01 GrantJ - Some SQLite builds/versions handle
        # the SQLITE_BUSY return value and connection parameter
        # "timeout" differently. For a more reliable duration,
        # manually retry the statement for 60 seconds. Only used
        # by statements which modify the database and do not use
        # a transaction (like those in ``__init__`` or ``reset``).
        # See Issue #85 for and tests/issue_85.py for more details.

        return self._retry(self.sql, statement, *args, **kwargs)

    def _retry(self, query, statement, *args, **kwargs) -> Any:
        """

        Args:
            query:
            statement:
            *args:
            **kwargs:

        Returns:

        """

        start: float = ct()
        while True:
            try:
                return query(statement, *args, **kwargs)
            except OperationalError as exc:
                if str(exc) != 'database is locked':
                    raise
                diff: float = ct() - start
                if diff > self._timeout:
                    raise TimeoutError(
                        'Query timeout >=60s. \n'
                        'statement: ``%s``, args: ``%s``, kwargs: ``%s`` \n'
                        'exception: ``%s``'
                        % (statement, args, kwargs, exc)
                    ) from exc
                sleep(0.001)

        # start = ct()
        # while True:
        #     try:
        #         try:
        #             ((old_value,),) = self.sql(
        #                 'PRAGMA %s' % (pragma)
        #             ).fetchall()
        #             update = old_value != value
        #         except ValueError:
        #             update = True
        #         if update:
        #             self.sql('PRAGMA %s = %s' % (pragma, value)).fetchall()
        #         break
        #     except OperationalError as exc:
        #         if str(exc) != 'database is locked':
        #             raise
        #         diff = ct() - start
        #         if diff > 60:
        #             raise
        #         sleep(0.001)

        # return value

    @contextmanager
    def _transact(self, retry=False):

        while True:
            try:
                self.sql('BEGIN IMMEDIATE')
                begin = True
                break
            except OperationalError:
                if retry:
                    continue

            try:
                yield self.sql
            except BaseException:
                if begin:
                    self.sql('ROLLBACK')
                raise
            else:
                if begin:
                    self.sql('COMMIT')

    @contextmanager
    def transact(self, retry=False):
        """Context manager to perform a transaction by locking the cache.

        While the cache is locked, no other write operation is permitted.
        Transactions should therefore be as short as possible. Read and write
        operations performed in a transaction are atomic. Read operations may
        occur concurrent to a transaction.

        Transactions may be nested and may not be shared between threads.

        Raises :exc:`Timeout` error when database timeout occurs and `retry` is
        `False` (default).

        >>> cache = Cache()
        >>> with cache.transact():  # Atomically increment two keys.
        ...     _ = cache.incr('total', 123.4)
        ...     _ = cache.incr('count', 1)
        >>> with cache.transact():  # Atomically calculate average.
        ...     average = cache['total'] / cache['count']
        >>> average
        123.4

        :param bool retry: retry if database timeout occurs (default False)
        :return: context manager for use in `with` statement
        :raises Timeout: if database timeout occurs

        """
        with self._transact(retry=retry):
            yield


if __name__ == '__main__':
    session = SQLiteSession('case.sqlite3', **PRAGMA_SETTINGS, isolation='exclusive')
    sql = session.sql
    #
    # s = sql('CREATE TABLE test(`id` integer primary key, `count` integer without rowid)').fetchall()
    # print(s)
    # s2 = sql('INSERT INTO test(id, count) values (1, 0)').fetchall()
    # print(s2)

    def get_value():
        s3 = sql('SELECT * FROM test').fetchall()
        print(s3)

    def reset():
        sql('UPDATE test SET `count`=0 WHERE `id`=?', (1,)).fetchone()

    def add():
        for i in range(10000):
            sql('UPDATE test SET `count`=`count`+1 WHERE `id`=?', (1,)).fetchone()

    def test_multiprocess():
        """"""

    def test_multithread():
        import threading
        ts = [threading.Thread(target=add) for _ in range(20)]
        [t.start() for t in ts]

    test_multithread()
    # reset()
    get_value()


import datetime
import os

# from heavy import special_commit


def modify():
    file = open('zero.md', 'r')
    flag = int(file.readline()) == 0
    file.close()
    file = open('zero.md', 'w+')
    if flag:
        file.write('1')
    else:
        file.write('0')
        file.close()


def commit():
    os.system('git commit -a -m test_github_streak > /dev/null 2>&1')


def set_sys_time(year, month, day):
    os.system('date -s %04d%02d%02d' % (year, month, day))


def trick_commit(year, month, day):
    set_sys_time(year, month, day)
    modify()
    commit()


def daily_commit(start_date, end_date):
    for i in range((end_date - start_date).days + 1):
        cur_date = start_date + datetime.timedelta(days=i)
        print(cur_date.year, cur_date.month, cur_date.day)


if __name__ == '__main__':
    # daily_commit(datetime.date(2015, 3, 31), datetime.date(2016, 1, 28))
    os.system('date -s %04d%02d%02d' % (2022, 1, 1))

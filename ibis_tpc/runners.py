#!/usr/bin/env python3

import glob
import itertools
import json
import os
import time
from pathlib import Path
from typing import List, Dict, Any

import click
import pandas
import ibis


def fmt(v):
    if isinstance(v, float):
        return '%.03f' % v
    else:
        return str(v)


def out_txt(s, outdir, fn):
    if outdir:
        print(s, file=open(Path(outdir)/fn, mode='w'), flush=True)


def out_sql(sql, outdir, fn):
    import sqlparse
    sql = sqlparse.format(str(sql), reindent=True, keyword_case='upper')

    out_txt(sql, outdir, fn)


def out_jsonl(rows: List[Dict[str, Any]], outdir, fn):
    class DateEncoder(json.JSONEncoder):
        def default(self, obj):
            if isinstance(obj, pandas.Timestamp):
                return str(obj)
            return json.JSONEncoder.default(self, obj)

    if outdir:
        with open(Path(outdir)/fn, mode='w') as fp:
            for r in rows:
                print(json.dumps(r, cls=DateEncoder), file=fp, flush=True)


class Runner:
    def __init__(self, interface='sqlite', backend='sqlite'):
        self.interface = interface
        self.backend = backend
        self.prints = []
        self.warns = []
        self.errors = []

    def setup(self, db='tpch.db'):
        self.prints = []
        self.warns = []
        self.errors = []

    def teardown(self):
        pass

    def run(self, qid, outdir=None, backend='sqlite'):
        pass

    def print(self, s):
        self.prints.append(s.strip())

    def warn(self, s):
        self.warns.append(s.strip())

    def error(self, s):
        self.errors.append(s.strip())

    def info(self):
        return dict(interface=self.interface, backend=self.backend)


class SqliteRunner(Runner):
    def setup(self, db='tpch.db'):
        super().setup(db=db)
        import sqlite3

        self.con = sqlite3.connect(db)
        self.con.row_factory = sqlite3.Row

    def run(self, qid, outdir=None, backend='sqlite'):
        cur = self.con.cursor()

        sql = open(f'sqlite_tpc/{qid}.sql').read()
        t1 = time.time()
        cur.execute(sql)
        rows = cur.fetchall()
        t2 = time.time()
        rows = list(dict(r) for r in rows)
        return rows, t2-t1

    def info(self):
        import sqlite3
        return dict(interface=self.interface,
                    backend=f'{self.backend}',
                    sqlite_version=sqlite3.sqlite_version)


class IbisRunner(Runner):
    def setup(self, db='tpch.db'):
        super().setup(db=db)
        self.con = getattr(ibis, self.backend).connect(db)

    def run(self, qid, outdir=None, backend='sqlite'):
        import importlib
        mod = importlib.import_module(f'.{qid}', package='ibis_tpc')
        q = getattr(mod, f'tpc_{qid}')(self.con)

        out_txt(repr(q), outdir, f'{qid}-{self.interface}-{self.backend}-expr.txt')

        out_sql(q.compile(), outdir, f'{qid}-{self.interface}-{self.backend}-compiled.sql')

        t1 = time.time()
        rows = q.execute()
        t2 = time.time()

        return rows.to_dict('records'), t2-t1


class RRunner(Runner):
    def setup(self, db='tpch.db'):
        super().setup(db=db)
        os.putenv('R_LIBS_SITE', '/usr/lib/R/library')  # skip warnings

        import rpy2
        import rpy2.robjects
        import rpy2.robjects.pandas2ri
        import rpy2.robjects.packages as rpackages

        rpy2.robjects.pandas2ri.activate()

        rpy2.rinterface_lib.callbacks.consolewrite_print = self.print
        rpy2.rinterface_lib.callbacks.consolewrite_warnerror = self.warn

        pkgs = ('dplyr', 'dbplyr', 'lubridate', 'DBI', 'RSQLite')
        names_to_install = [x for x in pkgs if not rpackages.isinstalled(x)]

        if names_to_install:
            from rpy2.robjects.vectors import StrVector
            utils = rpackages.importr('utils')
            utils.chooseCRANmirror(ind=1)  # select first mirror in the list
            utils.install_packages(StrVector(names_to_install))

        r = rpy2.robjects.r
        r['source']('dplyr_tpc/init.R')

        self.query_dbplyr = rpy2.robjects.globalenv['query_dbplyr']
        self.query_dplyr = rpy2.robjects.globalenv['query_dplyr']
        self.query_sql = rpy2.robjects.globalenv['query_sql']

        self.con = rpy2.robjects.globalenv['setup_sqlite'](db)

    def teardown(self):
        super().teardown()
        import rpy2.robjects
        rpy2.robjects.globalenv['teardown_sqlite'](self.con)

    def run(self, qid, outdir=None, backend='sqlite'):
        import rpy2.robjects

        r = rpy2.robjects.r
        fn = f'dplyr_tpc/{qid}.R'

        if not Path(fn).exists():
            raise FileNotFoundError(fn)

        r['source'](fn)
        func = rpy2.robjects.globalenv[f'tpc_{qid}']

        sql = self.query_sql(self.con, func)[0]
        out_sql(sql, outdir, f'{qid}-{self.interface}-{self.backend}.sql')

        t1 = time.time()
        res = rpy2.robjects.globalenv['query_'+self.interface](self.con, func)
        t2 = time.time()

        return res.to_dict('records'), t2-t1


setup_sqlite = SqliteRunner
setup_ibis = IbisRunner
setup_dplyr = RRunner
setup_dbplyr = RRunner


def compare(rows1, rows2):
    diffs = []
    for i, (r1, r2) in enumerate(itertools.zip_longest(rows1, rows2)):
        if r1 is None:
            diffs.append(f'[{i}]  extra row: {r2}')
            continue

        if r2 is None:
            diffs.append(f'[{i}]  extra row: {r1}')
            continue

        lcr1 = {k.lower(): v for k, v in r1.items()}
        lcr2 = {k.lower(): v for k, v in r2.items()}
        keys = set(lcr1.keys())
        keys |= set(lcr2.keys())
        for k in keys:
            v1 = lcr1.get(k, None)
            v2 = lcr2.get(k, None)
            if isinstance(v2, pandas.Timestamp):
                if v1 != v2.strftime('%Y-%m-%d'):
                    diffs.append(f'[{i}].{k} (date) {v1} != {v2}')
            elif isinstance(v1, float) and isinstance(v2, float):
                if v2 != v1:
                    if v1:
                        dv = abs(v2 - v1)
                        pd = dv/v1
                    else:
                        pd = 1
                    if pd > 1e-10:
                        diffs.append(f'[{i}].{k} (float) {v1} != {v2} ({pd*100}%)')

            else:
                if v1 != v2:
                    diffs.append(f'[{i}].{k} {v1} ({type(v1)}) != {v2} ({type(v2)})')

    return diffs


@click.command()
@click.argument('qids', nargs=-1)
@click.option('-d', '--db', default='tpch.db', help='connection string for db to run queries against')
@click.option('-b', '--backend', default='sqlite', help='backend to use with given db')
@click.option('-i', '--interface', 'interfaces', multiple=True, help='interface to use with backend: sqlite|ibis|dplyr|dbplyr')
@click.option('-o', '--output', 'outdir', type=click.Path(), default=None, help='directory to save intermediate and debug outputs')
@click.option('-v', '--verbose', count=True, help='display more information on stdout')
def main(qids, db, outdir, interfaces, backend, verbose):
    if outdir:
        os.makedirs(outdir, exist_ok=True)
        try:
            os.remove(Path(outdir)/'benchmarks.jsonl')
        except FileNotFoundError:
            pass
        try:
            os.remove(Path(outdir)/'benchmarks.txt')
        except FileNotFoundError:
            pass

    if not qids:
        qids = sorted(list(set(Path(fn).stem for fn in glob.glob('sqlite_tpc/*.sql') if '.' in fn)))

    if not interfaces:
        interfaces = ['sqlite', 'ibis', 'dplyr', 'dbplyr']

    runners = [globals()['setup_'+interface](interface=interface, backend=backend) for interface in interfaces]

    for qid in qids:
        results = []
        for runner, interface in zip(runners, interfaces):
            runner.setup(db)

            info = dict(qid=qid)
            info.update(runner.info())

            try:
                rows, elapsed_s = runner.run(qid, backend=backend, outdir=outdir)
                out_jsonl(rows, outdir, f'{qid}-{interface}-{backend}-results.jsonl')

                info['nrows'] = len(rows)
                info['elapsed_s'] = elapsed_s

                # first interface is baseline for correctness
                if results:
                    diffs = compare(results[0], rows)
                    out_txt('\n'.join(diffs), outdir, f'{qid}-{interface}-{backend}-diffs.txt')
                    info['ndiffs'] = len(diffs)

                results.append(rows)
            except KeyboardInterrupt:
                return
            except Exception as e:
                rows = []
                runner.error(type(e).__name__ + ': ' + str(e))

            if runner.errors:
                info['errors'] = '; '.join(runner.errors)

            if verbose > 0 and runner.warns:
                info['warns'] = '; '.join(runner.warns)

            if verbose > 1 and runner.prints:
                info['prints'] = '; '.join(runner.prints)

            if outdir:
                with open(Path(outdir)/'benchmarks.txt', mode='a') as fp:
                    print('  '.join(f'{k}:{fmt(v)}' for k, v in info.items()), file=fp)

                with open(Path(outdir)/'benchmarks.jsonl', mode='a') as fp:
                    print(json.dumps(info), file=fp)

            print('  '.join(f'{k}:{fmt(v)}' for k, v in info.items()))

            runner.teardown()


if __name__ == '__main__':
    main()

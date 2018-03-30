#!/usr/bin/env python
# -*- mode:python; coding:utf-8; -*-
# author: Eugene Zamriy <ezamriy@cloudlinux.com>
# created: 2018-01-30

"""
A distribution Yum repositories cleaning utility.
"""

import argparse
import functools
import re
import os
import shutil
import sqlite3
import subprocess
import sys
import yaml

import createrepo_c
from rpm import labelCompare
from rpmUtils.miscutils import splitFilename


class Channel(object):

    STABLE, BETA = range(2)

    @staticmethod
    def from_string(channel):
        if channel == 'stable':
            return Channel.STABLE
        elif channel == 'beta':
            return Channel.BETA
        raise ValueError(u'unsupported channel {0}'.format(channel))


class RemovalReason(object):

    EXPIRED_BY_STABLE, OUTDATED = range(2)

    @staticmethod
    def text(reason):
        if reason == RemovalReason.EXPIRED_BY_STABLE:
            return 'obsoleted by stable'
        elif reason == RemovalReason.OUTDATED:
            return 'outdated'


def create_repo(path, group_file=None):
    """
    Executes createrepo_c command for given directory.

    Parameters
    ----------
    group_file : str, optional
        Comps.xml file path.
    """
    cmd = ['createrepo_c', '--keep-all-metadata', '--compatibility',
           '--simple-md-filenames']
    if group_file:
        cmd.extend(('--groupfile', group_file))
    cmd.append(path)
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT)
    out, err = proc.communicate()
    if proc.returncode != 0:
        raise Exception(u"cannot createrepo {0}: {1}. Return code is {2}".
                        format(path, out, proc.returncode))


def normalize_path(path):
    """
    Returns an absolute pat with all variables expanded.

    Parameters
    ----------
    path : str
        Path to normalize.

    Returns
    -------
    str
        Normalized path.
    """
    return os.path.abspath(os.path.expanduser(os.path.expandvars(path)))


class DistroCleaner(object):

    def __init__(self, distro_config, backup_dir):
        self.__config = distro_config
        self.__backup_dir = os.path.join(backup_dir, distro_config['name'],
                                         str(distro_config['version']))
        keep_versions = distro_config.get('keep_versions', {})
        self.__keep_beta = keep_versions.get('beta', 3)
        self.__keep_stable = keep_versions.get('stable', 3)
        self.__exclude_re = None
        if 'exclude' in distro_config:
            self.__exclude_re = re.compile(distro_config['exclude'])
        self.__db = self.__create_db()
        self.__repos = {}
        self.__stats = {}

    def cleanup(self):
        for repo in self.__config['repositories']:
            self.__index_repo(repo)
        repos_by_arch = {}
        for row in self.__db.execute('SELECT * FROM repositories ORDER BY arch'):
            arch = row['arch']
            if arch not in repos_by_arch:
                repos_by_arch[arch] = {}
            repos_by_arch[arch][row['repo_id']] = {k: row[k] for k in row.keys()}
        for arch in repos_by_arch:
            if arch == 'src':
                continue
            print 'Cleaning up {0}.{1} {2} repositories'.\
                format(self.__config['name'], self.__config['version'], arch)
            self.__cleanup_arch(repos_by_arch[arch])
        self.__update_repodata()
        self.__print_report()

    def __cleanup_arch(self, repos):
        query = 'SELECT * FROM packages WHERE repo_id IN ({0}) ' \
                '  ORDER BY sourcerpm'.\
            format(', '.join([str(repo_id) for repo_id in repos.keys()]))
        last_srpm_name = None
        versions = {}
        for pkg_row in self.__db.execute(query):
            try:
                srpm_name, srpm_version, srpm_release, _, _ = \
                    splitFilename(pkg_row['sourcerpm'])
            except Exception as e:
                # TODO: usually a error here means that we're processing an
                #       src-RPM package which shouldn't be present in a binary
                #       repository. It makes sense to delete it automatically.
                repo = repos[pkg_row['repo_id']]
                pkg_path = os.path.join(repo['path'], pkg_row['location'])
                print >> sys.stderr, u'skipping invalid {0} package'.\
                    format(pkg_path)
                continue
            if last_srpm_name != srpm_name:
                if len(versions) > 1:
                    self.__cleanup_previous_versions(last_srpm_name, versions,
                                                     repos)
                last_srpm_name = srpm_name
                versions = {}
            version_key = (srpm_version, srpm_release)
            pkg = {k: pkg_row[k] for k in pkg_row.keys()}
            if version_key not in versions:
                versions[version_key] = [pkg]
            else:
                versions[version_key].append(pkg)

    def __cleanup_previous_versions(self, srpm_name, versions, repos):
        """
        Removes outdated versions of packages which were build from src-RPMs
        with an identical name.

        Parameters
        ----------
        srpm_name : str
            Src-RPM package name.
        versions : dict
            Package versions.
        repos : dict
            Repositories.
        """
        stable_repo_ids = [repo['repo_id'] for repo in repos.itervalues()
                           if repo['channel'] == Channel.STABLE]
        latest_stable_evr = None
        ord_versions = []
        # NOTE: here we're trying to re-arrange each version's packages to
        #       put a "main" package first because it's possible to build
        #       sub-packages with different EVR from the same src-RPM.
        for ver_packages in versions.itervalues():
            ver_packages.sort(
                cmp=lambda a, b: self.sort_by_rpm_name(srpm_name, a, b),
                reverse=True
            )
            ord_versions.append(ver_packages)
            main_package = ver_packages[0]
            main_package_evr = (main_package['epoch'], main_package['version'],
                                main_package['rel'])
            latest = False
            if latest_stable_evr and \
                    main_package['repo_id'] in stable_repo_ids and \
                    labelCompare((str(latest_stable_evr[0]),
                                  latest_stable_evr[1], latest_stable_evr[2]),
                                 (str(main_package_evr[0]), main_package_evr[1],
                                  main_package_evr[2])) < 0:
                latest = True
            if not latest_stable_evr or latest:
                latest_stable_evr = main_package_evr
        ord_versions.sort(lambda a, b: labelCompare((str(a[0]['epoch']),
                                                     a[0]['version'],
                                                     a[0]['rel']),
                                                    (str(b[0]['epoch']),
                                                     b[0]['version'],
                                                     b[0]['rel'])),
                          reverse=True)
        # remove any beta package which has a lower version than a stable one
        expired_beta = [v for v in ord_versions
                        if v[0]['repo_id'] not in stable_repo_ids and
                        labelCompare((str(latest_stable_evr[0]),
                                      latest_stable_evr[1],
                                      latest_stable_evr[2]),
                                     (str(v[0]['epoch']), v[0]['version'],
                                      v[0]['rel'])) > 0]
        if expired_beta:
            for packages in expired_beta:
                self.__cleanup_version(packages,
                                       reason=RemovalReason.EXPIRED_BY_STABLE)
                ord_versions.remove(packages)
        #
        beta_versions = []
        stable_versions = []
        for packages in ord_versions:
            if packages[0]['repo_id'] in stable_repo_ids:
                stable_versions.append(packages)
            else:
                beta_versions.append(packages)
        for packages in beta_versions[self.__keep_beta:]:
            self.__cleanup_version(packages, reason=RemovalReason.OUTDATED)
        for packages in stable_versions[self.__keep_stable:]:
            self.__cleanup_version(packages, reason=RemovalReason.OUTDATED)

    def __cleanup_version(self, packages, reason):
        repo_id = packages[0]['repo_id']
        repo = self.__repos[repo_id]
        if repo['readonly']:
            return
        backup_dir = os.path.join(self.__backup_dir, repo['name'], repo['arch'])
        if not os.path.exists(backup_dir):
            os.makedirs(backup_dir)
        if repo_id not in self.__stats:
            self.__stats[repo_id] = 0
        repo_text = u'{0} {1} {repo[name]}.{repo[arch]}'.\
            format(self.__config['name'], self.__config['version'], repo=repo)
        reason_text = RemovalReason.text(reason)
        for package in packages:
            package_path = os.path.join(repo['path'], package['location'])
            print u'deleting {0} package from {1} ({2})'.\
                format(package['location'], repo_text, reason_text)
            shutil.move(package_path, backup_dir)
            self.__stats[repo_id] += 1

    def __index_repo(self, repo):
        channel = Channel.from_string(repo['channel'])
        with self.__db:
            cur = self.__db.cursor()
            for arch, path in repo['path'].iteritems():
                if not os.path.exists(path):
                    raise Exception(u'{0} repository does not exist'.
                                    format(path))
                repo = {'name': repo['name'],
                        'arch': arch,
                        'path': path,
                        'channel': channel,
                        'readonly': repo.get('readonly', False)}
                cur.execute("""INSERT INTO repositories (name, arch, path,
                                                         channel, readonly)
                                      VALUES (?, ?, ?, ?, ?)""",
                            (repo['name'], arch, path, channel,
                             repo.get('readonly', False)))
                repo['repo_id'] = repo_id = cur.lastrowid
                self.__repos[repo_id] = repo
                self.__index_repo_packages(cur, repo_id, path)

    def __index_repo_packages(self, cur, repo_id, repo_path):
        """
        Extracts a packages list from a repository metadata and saves it to
        the database.

        Parameters
        ----------
        cur : sqlite3.Cursor
            Database cursor.
        repo_id : int
            Repository id.
        repo_path : str
            Repository path.
        """
        primary_xml_path = self.get_repomd_record_xml_path(repo_path, 'primary')
        save_cb = functools.partial(self.__save_repo_package, cur, repo_id)
        createrepo_c.xml_parse_primary(primary_xml_path, pkgcb=save_cb,
                                       do_files=False)

    def __save_repo_package(self, cur, repo_id, package):
        """
        Saves a package data to the database.

        Parameters
        ----------
        repo_id : int
            Repository id.
        package : createrepo_c.Package
            Package data.
        """
        if package.rpm_sourcerpm and \
                self.__is_srpm_excluded(package.rpm_sourcerpm):
            return
        cur.execute("""INSERT INTO packages (name, epoch, version, rel,
                                             arch, sourcerpm, location,
                                             repo_id)
                              VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                    (package.name, int(package.epoch), package.version,
                     package.release, package.arch, package.rpm_sourcerpm,
                     package.location_href, repo_id))

    @staticmethod
    def get_repomd_record_xml_path(repo_path, record_type):
        """
        Returns a file path of the specified repomd record.

        Parameters
        ----------
        repo_path : str
            Repository path.

        Returns
        -------
        str or None
            primary.xml file path or None if a record is not found in the
            repository metadata.
        """
        repomd_path = os.path.join(repo_path, 'repodata/repomd.xml')
        repomd = createrepo_c.Repomd(repomd_path)
        for rec in repomd.records:
            if rec.type == record_type:
                return os.path.join(repo_path, rec.location_href)

    def __update_repodata(self):
        for repo_id in self.__stats:
            repo = self.__repos[repo_id]
            print u'updating the {config[name]}-{config[version]} {repo[name]}.' \
                  u'{repo[arch]} repository metadata'.\
                format(config=self.__config, repo=repo)
            comps_xml_path = self.get_repomd_record_xml_path(repo['path'],
                                                             'group')
            create_repo(repo['path'], comps_xml_path)

    def __print_report(self):
        print 'Cleanup report:'
        for repo_id, count in self.__stats.iteritems():
            repo = self.__repos[repo_id]
            print u'{config[name]}-{config[version]} {repo[name]}.' \
                  u'{repo[arch]}: {0} packages deleted'.\
                format(count, config=self.__config, repo=repo)

    @staticmethod
    def sort_by_rpm_name(srpm_name, a, b):
        if a['name'] == srpm_name:
            return 1
        return -1 if b['name'] == srpm_name else 0

    def __is_srpm_excluded(self, srpm_name):
        """
        Checks if the given src-RPM package should be skipped.

        Parameters
        ----------
        srpm_name : str
            Src-RPM file name.

        Returns
        -------
        bool
            True if the src-RPM should be skipped, False otherwise
        """
        if self.__exclude_re and self.__exclude_re.search(srpm_name):
            return True
        return False

    def __create_db(self):
        schema = """
        CREATE TABLE repositories (
          repo_id   INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
          name      TEXT NOT NULL,
          arch      TEXT NOT NULL,
          path      TEXT NOT NULL,
          channel   INTEGER NOT NULL,
          readonly  BOOLEAN NOT NULL
        );
        
        CREATE TABLE packages (
          package_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
          name       TEXT NOT NULL,
          epoch      INTEGER,
          version    TEXT NOT NULL,
          rel        TEXT NOT NULL,
          arch       TEXT NOT NULL,
          sourcerpm  TEXT,
          location   TEXT NOT NULL,
          repo_id    INTEGER NOT NULL,
          FOREIGN KEY (repo_id) REFERENCES repositories(repo_id)
        );
        """
        db = sqlite3.connect(':memory:')
        db.row_factory = sqlite3.Row
        # db = sqlite3.connect('file:packages.db')
        db.executescript(schema)
        return db


def init_args_parser():
    """
    Command line arguments parser initialization.

    Returns
    -------
    argparse.ArgumentParser
    """
    parser = argparse.ArgumentParser(
        prog='yum_repo_cleaner',
        description='A distribution Yum repositories cleaning utility'
    )
    parser.add_argument('-c', '--config', help='configuration file path',
                        required=True)
    parser.add_argument('-b', '--backup-dir',
                        help='directory to store deleted packages. A "backup" '
                             'directory will be created in the current dir if'
                             'omitted')
    parser.add_argument('--distro-name',
                        help='distribution name. All distributions will be '
                             'processed if omitted')
    parser.add_argument('--distro-version',
                        help='distribution version. All versions will be '
                             'processed if omitted')
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='enable additional debug output')
    return parser


def main(sys_args):
    """
    Initializes configuration and starts a distribution cleaning.

    Parameters
    ----------
    sys_args : list
        Command line arguments list.

    Returns
    -------
    int
        Program exit code.
    """
    parser = init_args_parser()
    args = parser.parse_args(sys_args)
    with open(args.config, 'rb') as fd:
        config = yaml.safe_load(fd)
    backup_dir = args.backup_dir or normalize_path('./backup')
    if not os.path.exists(backup_dir):
        os.makedirs(backup_dir)
    for distro in config:
        if args.distro_name and distro['name'] != args.distro_name:
            continue
        elif args.distro_version and distro['version'] != args.distro_version:
            continue
        cleaner = DistroCleaner(distro, backup_dir)
        cleaner.cleanup()


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
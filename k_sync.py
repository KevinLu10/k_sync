# encoding=utf8
import sys
import os
import time
import logging
from watchdog.observers import Observer
from fabric.api import local, cd, run, env, hosts, put
from pathlib import Path, PurePath
import hashlib
from multiprocessing.dummy import Pool as ThreadPool
from watchdog.events import LoggingEventHandler, FileSystemEventHandler

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

CONF = dict(
    dir_map=[
        dict(
            remote_root='/data/watch_demo',  # 远端目录
            local_root='E:\watch_demo'  # 本地目录
        ),
    ],
    exclude=[
        '{remote_root}/abc/',  # 排除某个目录下面所有文件
        '{remote_root}/exclude.txt',  # 排除指定文件
        '{remote_root}/abc/*.txt',  # 排除目录下的某类文件
    ],
    host='192.168.44.128',  # 域名
    port='22',  # ssh端口
    user='root',  # ssh用户名
    pwd='pwdtest',  # ssh密码
    is_full_upd=1  # 是否开启全量同步
)


class WatchHandler(FileSystemEventHandler):
    """监听文件变化后的处理类"""

    def __init__(self, watcher, dir_conf):
        self.all_path_md5 = {}
        self.watcher = watcher
        self.dir_conf = dir_conf
        self.exclude = dir_conf['local_root']
        self.local_root = dir_conf['local_root']
        self.remote_root = dir_conf['remote_root']
        if self.watcher.conf['is_full_upd']:
            self.full_sync()

    def get_remote_md5(self):
        """获取远端目录的所有文件的md5"""
        cmd = 'find {remote_root} -type f  -print0 |xargs -0 md5sum '.format(remote_root=self.remote_root)
        ret = run(cmd)
        path_md5 = {}
        for line in ret.split('\n'):
            items = line.split()
            path_md5[items[1]] = items[0]

        return path_md5

    def get_all_local_md5(self, paths):
        """多协程获取本地所有文件的md5"""
        pool = ThreadPool(100)  # 线程池的大小，总并发数

        def f(param):
            path, is_dir = param
            if is_dir:
                return (path, '-1')
            return (path, self.get_local_md5(path))

        ret = pool.map(f, paths)
        return dict(ret)

    def get_local_md5(self, path):
        """获取本地单个文件的md5"""
        m = self.all_path_md5.get(path)
        if m is not None:
            return m
        with open(path, 'rb') as f:
            return hashlib.md5(f.read()).hexdigest()

    def get_all_path(self, root):
        """获取本地目录的所有文件路径，排除exclude配置的文件"""
        for base_dir, dirs, files in os.walk(root):
            remote_base_dir = self.local_to_remote_path(base_dir)
            if self.is_exclude(remote_base_dir):
                continue
            for d in dirs:
                path = os.path.join(base_dir, d)  # .decode('gbk').encode('utf8')
                yield path, 1
            for f in files:
                path = os.path.join(base_dir, f)  # .decode('gbk').encode('utf8')
                yield path, 0

    def full_sync(self):
        """
        全量更新
        先获取远端所有文件的md5，如果非目录，而且md5不匹配，或者远端没有文件，才同步
        """
        logging.info('start full sync %s', self.local_root)
        path_md5 = self.get_remote_md5()
        paths = list(self.get_all_path(self.local_root))
        self.all_path_md5 = self.get_all_local_md5(paths)
        for path, is_dir in paths:
            remote_path = self.local_to_remote_path(path)
            # logging.info('full sync %s is_dir=%s', remote_path, is_dir)

            if is_dir or path_md5.get(remote_path) != self.get_local_md5(path):
                if not is_dir:
                    logging.info('full sync %s is_dir:%s md5 diff ', remote_path, is_dir)
                self.sync_file(path, is_dir)
            else:
                pass
                # logging.info('full sync %s is_dir:%s md5 same , pass', remote_path, is_dir)
        logging.info('done full sync %s', self.local_root)

    def is_exclude(self, remote_path):
        """是否排除该文件"""
        if remote_path.endswith('__'):
            return 1
        for e in self.watcher.conf['exclude']:
            e = e.format(remote_root=self.remote_root)
            if PurePath(remote_path).match(e) or remote_path.startswith(e):
                return 1
        return 0

    def to_gbk(self, path):
        """转换路径为gbk编码"""
        if isinstance(path, unicode):
            path = path.encode('gbk')
        else:
            try:
                path = path.decode('utf8').encode('gbk')
            except:
                pass
        return path

    def to_utf8(self, path):
        """转换路径为utf8编码"""
        if isinstance(path, unicode):
            path = path.encode('utf8')
        else:
            try:
                path = path.decode('gbk').encode('utf8')
            except:
                pass
        return path

    def local_to_remote_path(self, local_path):
        """转换路径为远端路径"""
        local_path = self.to_utf8(local_path)
        remote_path = local_path.replace(self.local_root, self.remote_root, )
        remote_path = remote_path.replace('\\', '/')
        return remote_path

    def sync_file_action(self, is_dir, path, remote_path):
        """通过fabric执行同步文件或者文件夹操作"""
        path = self.to_gbk(path)
        remote_path = self.to_utf8(remote_path)

        if is_dir:
            ret = run('mkdir -p %s ' % remote_path)
            logging.info('mkdir  %s result : %s' % (remote_path, ret))
        else:
            ret = put(path, remote_path).failed
            logging.info('sync file %s to %s result : %s' % (path, remote_path, ret))

    def sync_file(self, path, is_dir):
        """同步文件或者文件夹"""
        if not path.startswith(self.local_root):
            logging.error('path is not in target dir path:%s local_root:%s', path, self.local_root)
            return 0
        remote_path = self.local_to_remote_path(path)
        if self.is_exclude(remote_path):
            logging.info('exclude %s' % (remote_path))
            return -1
        self.sync_file_action(is_dir, path, remote_path)

    def on_moved(self, event):
        """watchdog回调方法，当文件被移动调用"""
        what = 'directory' if event.is_directory else 'file'
        logging.info("Moved %s: from %s to %s", what, event.src_path, event.dest_path)
        self.sync_file(event.dest_path, event.is_directory)

    def on_created(self, event):
        """watchdog回调方法，当文件被创建调用"""
        what = 'directory' if event.is_directory else 'file'
        logging.info("Created %s: %s", what, event.src_path)
        self.sync_file(event.src_path, event.is_directory)

    def on_deleted(self, event):
        """watchdog回调方法，当文件被删除调用"""
        what = 'directory' if event.is_directory else 'file'
        logging.info("Deleted %s: %s", what, event.src_path)
        # self.sync_file(event.src_path, event.is_directory)

    def on_modified(self, event):
        """watchdog回调方法，当文件被修改调用"""
        what = 'directory' if event.is_directory else 'file'
        logging.info("Modified %s: %s", what, event.src_path)
        self.sync_file(event.src_path, event.is_directory)


class Watcher():
    """监听类"""

    def __init__(self, conf):
        self.conf = conf
        self.set_fabric_env()
        self.watch_dir()

    def set_fabric_env(self):
        """设置fabric环境变量"""
        env.host_string = '%s@%s:%s' % (self.conf['user'], self.conf['host'], self.conf['port'],)
        env.user = ''
        env.password = self.conf['pwd']

    def watch_dir(self):
        """监听目录"""
        for m in self.conf['dir_map']:
            event_handler = WatchHandler(self, m)
            observer = Observer()
            observer.schedule(event_handler, m['local_root'], recursive=True)
            observer.start()
            m['handler'] = event_handler
            m['observer'] = observer

    def loop(self):
        """循环"""
        while 1:
            time.sleep(1)


def test():
    """单元测试"""
    watcher = Watcher(CONF)
    h = watcher.conf['dir_map'][0]['handler']
    # print h.get_remote_md5()
    # print h.is_exclude('/data/watch_demo/exclude.txt')
    # print h.is_exclude('/data/watch_demo/abc/')
    # print h.is_exclude('/data/sb.watch/.git/aa')
    # print h.is_exclude('/data/sb.watch/app.py')


if __name__ == '__main__':
    watcher = Watcher(CONF)
    watcher.loop()
    # test()

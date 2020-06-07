# k_sync
该项目利用fabric和watchdog两个库，实现自动同步本地目录文件到服务器。这样在Window开发的代码就能快速自动地同步到Linux服务器，然后进行调试。类似Pycharm的Deployment的功能。

### 快速开始
1.安装依赖库 
`pip install fabric `  
`pip install watchdoy`

2.修改配置，也就是r_sync文件的CONF变量。设置本地目录和远端目录的映射，排除的文件，远端服务器的登录SSH信息等。具体说明见代码的注释。

3.执行文件 `python k_rsync.py`

执行后，如果设置了is_full_upd=1，代码会先进行一次全量同步。md5相同的文件会被排除，以提升速度。  
进程会一直监听目录里面文件的变化，监听到变化后会通过fabric，上传本地文件到远程服务器，以完成同步的操作。


### 其他
* 本项目只支持在window执行（主要是gbk和utf8编码问题）。
* Python版本是2.7。

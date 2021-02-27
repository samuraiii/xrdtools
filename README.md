# xrdtools
Tools for administering of XRootD storage elements
Supported is only python3 compatible release.
This is still a work in progress, so you can expect some problem, but none of them should cause major data loss.
There is Python3v1.0 tag to denote python3 support, but I would recommend to use "master" branch as you code source.


# Known Issues
The default sshd connection limit (10) can cause that no more connections can be made to the taget server.
The default connection count in multithreaded (also default) run is "cpu count" * 2.
This can saturate connection pool in one instance on 6 core source machine.
Please temporarly adjust `MaxSessions`  in `/etc/ssh/sshd_config` to something bigger than number of connections you are going to use durting the drain.



# Python2
You will find python2 compatible version uder the Python2 tag:
https://github.com/samuraiii/xrdtools/tree/Python2
This version is no longer supported.

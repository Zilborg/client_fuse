## FUSE client
The working of FUSE client was checked with Nautilus (version 1:3.20.4-0ubuntu2) on ubuntu 17.04.

By default you have two files:`fuse.py` and `start_fuse.py`. You should start from `start_fuse.py`. Also by default, the root directory is created in current directory(where this two file locates). The mountpoin creates the directory in current directory as `DevilStore`. Thus the cache files, that were downloaded during the session are cleared after restarting the computer. You can avoid this by changing the `ROOT = "/tmp/fuse"` in the `start_fuse.py`.
### Process

#### In the beginning:

When the script starts, first of all, the tree of directory loads for identify user. According to this information, the files with 0 size are created. However, in the mount directory, you can see the virtual size.

#### In the process:

When the user opens the file, if the file was not downloaded, it downloads from the server. In this moment the Nautilus is opening the file.

When the file is loading on a server the user can only read, without write.

All processing information you can see in the terminal, where the script starts. If you want to see more information you can change the `DEBUG` from `False` to `True`.

During the session, the user sends keepalive packets. Thus, another user can not user log/pass that is online.

#### In the end:

When a user closes the process the packet with "closing information" sends to Name Server. After this, you can reconnect. If the closing packet did not send by automatic, the session closes after 60 secs.

### You can change:
```
DEBUG = False
LOGS = False
```
Where `DEBUG` shows information by working Nautilus and `LOGS` creates `logs.txt`, if it necessary. Also all processing information you can see in terminal.

### Exit
For exiting you need use double interupts. 2 x `Ctrl + C` (^C) 

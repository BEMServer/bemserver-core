Deployment documentation
========================

Create bemserver user and required directories.

    $ adduser --system --home /var/run/bemserver --group bemserver

    $ mkdir -m 0750 /var/log/bemserver
    $ chown bemserver:bemserver /var/log/bemserver

Copy `etc` and `srv` to server `/`.

Then edit configuration files. At least the lines marked with a `TODO` comment.

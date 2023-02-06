Deployment documentation
========================

Create bemserver user and required directories.

    $ adduser --system --home /var/run/bemserver --group bemserver

    $ mkdir -m 0750 /var/log/bemserver
    $ chown bemserver:bemserver /var/log/bemserver

Copy etc directory's content into /etc and edit config files.

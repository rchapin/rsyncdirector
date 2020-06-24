# This software is released under the Revised BSD License.
# See LICENSE for details
#
# Copyright (c) 2019, Ryan Chapin, https//:www.ryanchapin.com
# All rights reserved.

FROM alpine:3.23.2

ARG root_passwd

RUN apk add --no-cache \
    openssh \
    rsync \
    # To get full 'ps' functionality
    procps \
    # Ensure that we can passwordlessly ssh to this host
    && sed -i '/PermitRootLogin/d' /etc/ssh/sshd_config \
    && echo "PermitRootLogin yes" >> /etc/ssh/sshd_config \
    # Generate keys for the ssh server
    && ssh-keygen -A \
    # Create a known password for the root user
    && echo "root:$root_passwd" | chpasswd \
    # Create an .ssh dir for the root user and set the permissions
    && mkdir /root/.ssh \
    && chown root: /root/.ssh \
    && chmod 700 /root/.ssh \
    # Create a directory into which we will sync data for the tests
    && mkdir /data

COPY id_rsa.pub /root/.ssh/authorized_keys
RUN chmod 600 /root/.ssh/authorized_keys

EXPOSE 22
CMD ["/usr/sbin/sshd","-D"]


#
# COPYRIGHT Fujitsu Software Technologies Limited 2018 All Rights Reserved.
#

#
# Create a custom docker image for BlockArchiver container
# This dockerfile is base on https://docs.docker.com/engine/examples/running_ssh_service/.
# Port "222" is exposed for accessing to BlockArchiver
#

FROM ubuntu:16.04

RUN apt-get update && apt-get install -y openssh-server
RUN mkdir /var/run/sshd
RUN echo 'root:blkstore' | chpasswd
RUN sed -i 's/PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config

# SSH login fix. Otherwise user is kicked off after login
RUN sed 's@session\s*required\s*pam_loginuid.so@session optional pam_loginuid.so@g' -i /etc/pam.d/sshd

ENV NOTVISIBLE "in users profile"
RUN echo "export VISIBLE=now" >> /etc/profile

# EXPOSE 22
CMD ["/usr/sbin/sshd", "-D", "-p", "222"]

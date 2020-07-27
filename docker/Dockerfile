# This example uses rstudio r-base docker images: https://github.com/rstudio/r-docker
FROM rstudio/r-base:4.0.2-centos7

# Set up centos
RUN sudo yum install -y https://repo.ius.io/ius-release-el7.rpm \
   && sudo yum update -y
   
# get aws cli
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" \
    && unzip awscliv2.zip \
    && sudo ./aws/install

# install python 3.6
RUN sudo yum install -y python36u python36u-libs python36u-devel python36u-pip

# install boto3
RUN pip3 install --user boto3 numpy

# Install RAthena
RUN R -e "install.packages('RAthena', repos='https://cran.rstudio.com/')"

CMD ["R"]

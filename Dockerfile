FROM python:3.8-slim

LABEL maintainer="DevOps (GALE Partners LP) devops@galepartners.com"

# # Never prompts the user for choices on installation/configuration of packages
ENV DEBIAN_FRONTEND noninteractive
# ENV TERM linux
ENV APPUSER_HOME /home/appuser
ENV APP_HOME /application
# ENV C_FORCE_ROOT=true
# ENV PYTHONUNBUFFERED 1

# Define en_US.
ENV LANGUAGE en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LC_CTYPE en_US.UTF-8
ENV LC_MESSAGES en_US.UTF-8


RUN set -ex \
    && buildDeps=' \
    libpq-dev \
    python3-dev' \
    && apt-get update -yqq \
    && apt-get install -yqq --no-install-recommends \
    $buildDeps \
    build-essential \
    jq \
    procps \
    awscli \
    openssh-client \
    git \
    curl

RUN apt-get -y install wkhtmltopdf

RUN useradd -m -u 10010 appuser

WORKDIR $APP_HOME

# Copy code into Image
ADD . $APP_HOME

# Copy keys


USER appuser


# Install pip packages
RUN pip install --user -r requirements.txt

ENV PATH="/home/appuser/.local/bin:${PATH}"

# # Clean up keys
# RUN rm -rf id_rsa
# RUN rm -rf $HOME/.ssh

# # Clean up Build Files
# RUN rm -rf $APP_HOME/.Build-Dir*

EXPOSE 8000 6955

WORKDIR ${APP_HOME}


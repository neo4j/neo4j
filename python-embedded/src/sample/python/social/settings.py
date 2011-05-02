# -*- mode: Python; coding: utf-8 -*-

# Copyright (c) 2002-2011 "Neo Technology,"
# Network Engine for Objects in Lund AB [http://neotechnology.com]
#
# This file is part of Neo4j.
#
# Neo4j is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.


# Django settings for the 'social' project.

import os

DEBUG = True
TEMPLATE_DEBUG = DEBUG

ADMINS = (
    # ('Your Name', 'your_email@domain.com'),
)

MANAGERS = ADMINS

# Figure out base directories (assume we have the entire repo)
_root = os.path.dirname( # neo4j-python
    os.path.dirname( # src
        os.path.dirname( # sample
            os.path.dirname( # python
                os.path.dirname( # social
                    os.path.abspath(__file__))))))
_resources = os.path.join(
    os.path.dirname( # sample
        os.path.dirname( # python
            os.path.dirname( # social
                os.path.abspath(__file__)))), 'resources')

# Configure the Databases, configure Neo4j
DATABASES = {
    'default': { # May not be 'default' - because that implies stuff to Django
        'ENGINE': 'neo4j', # Do not prepend 'django.db.backends.'
        # Path for where the Neo4j database is located
        'PATH': os.path.join(_root,'target','sample-data','social-db'),
    }
}

# Local time zone for where Neo4j is (mainly) developed.
TIME_ZONE = 'Europe/Stockholm'

LANGUAGE_CODE = 'en-us'

SITE_ID = 1

USE_I18N = True

USE_L10N = True

MEDIA_ROOT = os.path.join(_root,'target','sample-data','social-mediaroot')

MEDIA_URL = ''

ADMIN_MEDIA_PREFIX = '/media/'

# Make this unique, and don't share it with anybody.
SECRET_KEY = 'e!zvfvuv6l)-oc#m@loh#@u@!d+rjqxw2dsyvfjbv(=&q#dq4('

# List of callables that know how to import templates from various sources.
TEMPLATE_LOADERS = (
    'django.template.loaders.filesystem.Loader',
    'django.template.loaders.app_directories.Loader',
#     'django.template.loaders.eggs.Loader',
)

MIDDLEWARE_CLASSES = (
    'django.middleware.common.CommonMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
)

ROOT_URLCONF = 'social.urls'

TEMPLATE_DIRS = (
    os.path.join(_resources, 'social'),
)

INSTALLED_APPS = (
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.sites',
    'django.contrib.messages',
    # Uncomment the next line to enable the admin:
    # 'django.contrib.admin',
    # Uncomment the next line to enable admin documentation:
    # 'django.contrib.admindocs',
)

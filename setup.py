# -*- coding: utf-8 -*-
"""Setup for Drastic Jobs.
"""
import inspect
import os
from pip.req import parse_requirements
try:
    from pip.download import PipSession
except ImportError:
    # Old pip
    pass
from setuptools import setup, find_packages
# Import Setuptools
# from ez_setup import use_setuptools
# use_setuptools()
# Import our own module here for version number
import jobs


__copyright__ = "Copyright (C) 2016 University of Maryland"
__license__ = "GNU AFFERO GENERAL PUBLIC LICENSE, Version 3"


# Inspect to find current path
setuppath = inspect.getfile(inspect.currentframe())
setupdir = os.path.dirname(setuppath)

# Find longer description from README
#with open(os.path.join(setupdir, 'README.rst'), 'r') as fh:
#    _long_description = fh.read()

# Requirements
with open(os.path.join(setupdir, 'requirements.txt'), 'r') as fh:
    try:
        raw_reqs = parse_requirements('requirements.txt', session=PipSession())
    except NameError:
        # Old pip
        raw_reqs = parse_requirements('requirements.txt')

    _install_requires = [
        str(req.req)
        for req
        in raw_reqs
        if req
    ]


setup(
    name='drastic-jobs',
    version=jobs.__version__,
    description='Drastic Jobs',
    packages=find_packages(),
    install_requires=_install_requires,
    long_description='',
    author='jansen@umd.edu',
    maintainer_email='jansen@umd.edu',
    license="GNU AFFERO GENERAL PUBLIC LICENSE, Version 3",
    url='https://github.com/UMD-DRASTIC/drastic-cli',
    setup_requires=['setuptools-git'],
    entry_points={
        'console_scripts': [
            # "ingest_httpftp = scripts:ingest_httpftp:main"
        ]
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "License :: OSI Approved :: GNU Affero General Public License v3",
        "Programming Language :: Python :: 2.7",
        "Topic :: Internet :: WWW/HTTP :: WSGI",
        "Topic :: Internet :: WWW/HTTP :: WSGI :: Application",
        "Topic :: Internet :: WWW/HTTP :: WSGI :: Middleware",
        "Topic :: System :: Archiving"
    ],
)

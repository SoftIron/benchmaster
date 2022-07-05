#!/usr/bin/env python

"""The setup script."""

from setuptools import setup, find_packages

with open('README.md') as readme_file:
    readme = readme_file.read()

requirements = [
    'boto',
    'gspread',
    'docopt',
]

test_requirements = [ ]

setup(
    python_requires='>=3.6',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: GNU General Public License v2 (GPLv2)',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    description="Python tool to coordinate storage benchmarking tests",
    entry_points={
        'console_scripts': [
            'benchmaster=benchmaster.benchmaster:main',
        ],
    },
    install_requires=requirements,
    license="GNU General Public License v2",
    long_description=readme,
    include_package_data=True,
    keywords='benchmaster',
    name='benchmaster',
    packages=find_packages(include=['benchmaster', 'benchmaster.*']),
    test_suite='tests',
    tests_require=test_requirements,
    url='https://github.com/softiron/benchmaster',
    version='1.0.0',
    zip_safe=False,
)
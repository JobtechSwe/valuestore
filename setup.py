from setuptools import setup, find_packages

setup(
    name='valuestore',
    version='1.1.0',
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        'elasticsearch'
    ],
    tests_require=["pytest"]
)

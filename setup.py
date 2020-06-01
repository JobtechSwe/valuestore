from setuptools import setup, find_packages

setup(
    name='valuestore',
    version='1.0.0',
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        'elasticsearch'
    ],
    setup_requires=["pytest-runner"],
    tests_require=["pytest"]
)

from setuptools import setup, find_packages

setup(
    name='valuestore',
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        'elasticsearch'
    ],
    setup_requires=["pytest-runner"],
    tests_require=["pytest"]
)

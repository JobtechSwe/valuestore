from setuptools import setup

setup(
    name='valuestore',
    packages=['valuestore'],
    include_package_data=True,
    install_requires=[
        'elasticsearch'
    ],
    setup_requires=["pytest-runner"],
    tests_require=["pytest"]
)

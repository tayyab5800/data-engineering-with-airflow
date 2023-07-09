from setuptools import setup, find_packages

setup(
    name='iibflix',
    version='0.1.0',
    packages=find_packages(),
    install_requires=[
        'click',
        'pandas',
    ],
    entry_points='''
        [console_scripts]
        iibflix=iibflix.cli:iibflix
    '''
)

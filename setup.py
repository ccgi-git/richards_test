# This is where you run your arbitrary code. This is what will allow us to install our package

# to intiate your package, select your virtualenv and type
# pip install -e .
# the -e command intalls a link to your package, so if there are changes to the
# file/package, you don't have to reinstall the entire package
from setuptools import setup

if __name__=="__main__":
    setup()
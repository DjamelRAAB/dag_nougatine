from setuptools import setup, find_packages

def readme():
    with open('README.md') as f:
        return f.read()
print(find_packages())

setup(name='apispotify',
      version='0.1',
      description='apiformation',
      long_description=readme(),
      url='https://github.com/DjamelRAAB/dag_nougatine/apispotify',
      author='Djamel RAAB',
      author_email='djamel.r.75@gmail.com',
      packages=find_packages(),
      install_requires=[
          'spotipy',
          'pandas'         
      ]
     )
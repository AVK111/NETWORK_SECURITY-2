'''
The setup.py is an essential part of packaging and distribution python projects. It is used by setuptools
(or disutils in older python versions) to define th configuration of your project
'''
from setuptools import find_packages, setup
from typing import List

def get_requirements() -> List[str]:
    """
    Docstring for get_requirements
    
    :return: List of requirement
    """
    requirement_lst:List[str]=[]
    try:
        with open('requirements.txt','r') as file:
            lines=file.readlines()
            for line in lines:
                requirement=line.strip()
                if requirement and requirement!= '-e .':
                    requirement_lst.append(requirement)
    except FileNotFoundError:
        print("requirements.txt file not found")

    return requirement_lst
print(get_requirements())

setup(
    name="NetworkSecurity",
    version="0.0.1",
    author="Atharv Kale",
    author_email="atharvakale9696@gmail.com",
    packages=find_packages(),
    install_requires=get_requirements()
)
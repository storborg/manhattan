from setuptools import setup


setup(name="manhattan",
      version='0.1',
      description='',
      long_description='',
      classifiers=[
        'Programming Language :: Python :: 2.7',
      ],
      keywords='',
      author='Scott Torborg',
      author_email='scott@cartlogic.com',
      install_requires=[
          'gzlog'
      ],
      license='PRIVATE',
      packages=['manhattan'],
      test_suite='nose.collector',
      tests_require=['nose'],
      zip_safe=False)

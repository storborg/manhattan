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
          'sqlalchemy>=0.7',
          'itsdangerous',
          'webob',
          'pytz',
          'pyzmq',
          'simplejson',
          # These are for tests.
          'coverage',
          'nose>=1.1',
          'nose-cover3',
          'webtest',
      ],
      license='PRIVATE',
      packages=['manhattan'],
      entry_points=dict(console_scripts=[
          'manhattan-server=manhattan.server:main',
          'manhattan-client=manhattan.client:main']),
      test_suite='nose.collector',
      tests_require=['nose'],
      zip_safe=False)

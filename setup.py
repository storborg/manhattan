from setuptools import setup


setup(name="manhattan",
      version='0.2',
      description='Robust Server-Side Analytics',
      long_description='',
      classifiers=[
          'Development Status :: 5 - Production/Stable',
          'Programming Language :: Python :: 2.7',
          'Topic :: Internet :: WWW/HTTP :: WSGI :: Middleware',
          'Topic :: Internet :: WWW/HTTP :: Site Management',
      ],
      keywords='',
      url='http://github.com/cartlogic/manhattan',
      author='Scott Torborg',
      author_email='scott@cartlogic.com',
      install_requires=[
          'sqlalchemy>=0.7',
          'itsdangerous',
          'webob',
          'pytz',
          'pyzmq',
          # These are for tests.
          'coverage',
          'nose>=1.1',
          'nose-cover3',
          'webtest',
      ],
      license='MIT',
      packages=['manhattan'],
      entry_points=dict(console_scripts=[
          'manhattan-server=manhattan.server:main',
          'manhattan-client=manhattan.client:main']),
      test_suite='nose.collector',
      tests_require=['nose'],
      zip_safe=False)

from setuptools import setup, find_packages


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
          'webob',
          'redis>=2.7.2',
          'pytz',
          'pyzmq',
          'simplejson',
      ],
      license='MIT',
      packages=find_packages(),
      entry_points=dict(
          console_scripts=[
              'manhattan-server=manhattan.server:main',
              'manhattan-client=manhattan.client:main',
              'manhattan-log-server=manhattan.log.remote:server',
          ]
      ),
      test_suite='nose.collector',
      tests_require=['nose'],
      zip_safe=False)

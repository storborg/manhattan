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
      install_requires=[],
      license='PRIVATE',
      packages=['manhattan'],
      entry_points=dict(console_scripts=[
          'manhattan-worker=manhattan.worker:main']),
      test_suite='nose.collector',
      tests_require=['nose'],
      zip_safe=False)

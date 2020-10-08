from setuptools import setup

setup(name='githubcheck',
      version='0.1',
      description='create/update github check run',
      classifiers=[
        'Development Status :: 3 - Alpha',
        'Environment :: Console',
        'Intended Audience :: Information Technology',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
      ],
      url='http://github.com/ceph/cbt',
      author='Kefu Chai',
      author_email='kchai@redhat.com',
      license='MIT',
      packages=['githubcheck'],
      install_requires=[
          'github3.py',
          'jinja2',
      ],
      zip_safe=False,
      scripts=['bin/github-check'])

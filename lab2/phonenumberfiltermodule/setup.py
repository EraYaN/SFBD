from setuptools import setup, Extension
import sys

if sys.platform == 'darwin':
    module1 = Extension('phonenumberfilter',
                    sources=['phonenumberfiltermodule.cpp','timing.cpp'],
                    include_dirs=['/usr/local/Cellar/zlib/1.2.11/include', '../gzstream/'],
                    libraries=['z'],
                    library_dirs=['/usr/local/Cellar/zlib/1.2.11/lib'])
elif sys.platform == 'win32':
    module1 = Extension('phonenumberfilter',
                    sources=['phonenumberfiltermodule.cpp','timing.cpp'],
                    include_dirs=['../zlib-1.2.11/', '../gzstream/'],
                    libraries=['zlibstat'],
                    library_dirs=['../zlib-1.2.11/contrib/vstudio/vc14/x64/ZlibStatRelease'])
else:
    module1 = Extension('phonenumberfilter',
                    sources=['phonenumberfiltermodule.cpp','timing.cpp'],
                    include_dirs=['gzstream/'],
                    libraries=['z'],
                    library_dirs=[],
                    extra_compile_args = ["-fprefetch-loop-arrays", "-std=c++11", "-O3"])

setup (name='PhoneNumberFilter',
       version='1.3.0',
       description='This is a package to quickly filter dutch phone numbers in international format from a WARC WET file, now with compression support.',
       zip_safe=False,
       ext_modules=[module1])

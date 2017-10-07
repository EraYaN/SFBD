from setuptools import setup, Extension

module1 = Extension('phonenumberfilter',
                    sources = ['phonenumberfiltermodule.cpp'],
                    include_dirs = ['../zlib-1.2.11/','../gzstream/'],
                    libraries = ['zlibstat'],
                    library_dirs = ['../zlib-1.2.11/contrib/vstudio/vc14/x64/ZlibStatRelease'])

setup (name = 'PhoneNumberFilter',
       version = '1.1',
       description = 'This is a package to quickly filter dutch phone numbers from a WARC WET file, now with compression support.',
       ext_modules = [module1])
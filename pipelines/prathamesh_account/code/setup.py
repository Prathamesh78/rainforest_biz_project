from setuptools import setup, find_packages
setup(
    name = 'prathamesh_account',
    version = '1.0',
    packages = find_packages(include = ('prathamesh_account*', )) + ['prophecy_config_instances'],
    package_dir = {'prophecy_config_instances' : 'configs/resources/config'},
    package_data = {'prophecy_config_instances' : ['*.json', '*.py', '*.conf']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==1.9.33'],
    entry_points = {
'console_scripts' : [
'main = prathamesh_account.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html', 'pytest-cov'], }
)

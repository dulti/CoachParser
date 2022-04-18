from environs import Env

env = Env()
env.read_env()

PG_USER = env.str('PG_USER')
PG_DATABASE = env.str('PG_DATABASE')

EXPORT_PATH = env.str('EXPORT_PATH')

import os
import sys
from components.utils.misc import createDirIfNotExists
#############################################################################
# default options
define_options = (
    ("address", "127.0.0.1", str, "listen address"),
    ("port", 80, int, "listen port"),
    ("debug", False, bool, "enable debug mode"),
    ("static_path", "web/static", str, "path of static folder"),
    ("template_path", "web", str, "path of template folder"),
    ("note", "", str, "service descriptoin"),
    ("cookie_secret", "", str, "to encrypt cookie"),
    ("forks", 0, int, "fork process to use all cpu core"),
    ("compress_response", True, bool, "compress response content"),
    ("login_url", "/login", str, "the url will be used to redirect for user login"),
    ("mysql_config", "", dict, "the mysql database config"))
#############################################################################

# tornado settings NOT  MODULE SETTINGS
template_path = "./web"
static_path = "./web/static"
cookie_secret = 'MEZzzzzzl4NkRWFtb3zzzzg3Y1JMZm5IMnBDcZEXOVhCNXNzzzzRWXJ6ax2d0pzzzz='
xsrf_cookies = True
compress_response = True
login_url = '/login'
address = ''
port = 80
forks = 1   # 0: forks one process per cpu
# use X-Real-IP (if there is) to get real remote ip instead of lbs' ip address
xheaders = True
logging = 'debug' if any(
    filter(lambda x: x.lower().find('debug') >= 0, sys.argv)) else 'info'
debug = True if any(
    filter(lambda x: x.lower().find('debug') >= 0, sys.argv)) else False


# log config
log_rotate_mode = 'size'  # time or size
log_file_max_size = 20*1024*1024
log_file_num_backups = 100
# log_rotate_when='M' # 单位: S / M / H / D / W0 - W6
# log_rotate_interval='20'

log_dir = './log' if os.name == 'nt' else '/nas/log'
log_file_prefix = f'{log_dir}/app_log'
log_to_stderr = True if any(
    filter(lambda x: x.lower().find('debug') >= 0, sys.argv)) else False

createDirIfNotExists(log_dir)

mysql_pool_config={
    'pool_max_size':20,
    'pool_recycle_time': 60
}
mysql_config={
    'host':'rm-uf642102c6905a2xneo.mysql.rds.aliyuncs.com',
    'port':3306,
    'user':'linku',
    'password':'linku!@#$4321'
}
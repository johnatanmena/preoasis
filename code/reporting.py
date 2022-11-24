import logging
import json
from transversal_classes import ProjectParameters

params = ProjectParameters()
CLEVEL = int(params.getconsoledebuglevel())
FLEVEL = int(params.getfiledebuglevel())
#logging configuration
logging.basicConfig(level = 10,
  format   = '%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
  datefmt  = '%Y-%m-%d %H:%M:%S',
  filename = params.LOG_FILES + 'complete_log_info.log',
  filemode = 'w')

console = logging.StreamHandler()
console.setLevel(CLEVEL) #change the severity level in this line so the console doesn't show verbose
formatter = logging.Formatter('%(asctime)s %(name)-12s: %(levelname)-8s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
console.setFormatter(formatter)

file_log = logging.FileHandler(params.LOG_FILES + 'execution_info.log', mode='w')
file_formatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
file_log.setFormatter(file_formatter)
file_log.setLevel(FLEVEL)

logging.getLogger('').addHandler(console)
logging.getLogger('').addHandler(file_log)

#CREATE LOGGERS FOR EACH SECTION ON CODE
roo_logger = logging.getLogger('')
ext_logger = logging.getLogger('extract')
trn_logger = logging.getLogger('transform')
enr_logger = logging.getLogger('enrichment')
rep_logger = logging.getLogger('report')
loa_logger = logging.getLogger('load')
pub_logger = logging.getLogger('publish')


ext_logger.setLevel(FLEVEL) #change this variables to handle verbose data
trn_logger.setLevel(FLEVEL) 
loa_logger.setLevel(FLEVEL)
rep_logger.setLevel(FLEVEL)
enr_logger.setLevel(FLEVEL)
pub_logger.setLevel(FLEVEL)

ext_logger.addHandler(file_log) #add file handler for each logger
trn_logger.addHandler(file_log) 
loa_logger.addHandler(file_log)
rep_logger.addHandler(file_log)
enr_logger.addHandler(file_log)
pub_logger.addHandler(file_log)

log = logging.getLogger('werkzeug') # server log level to ERROR supress verbose 
log.setLevel(logging.ERROR)
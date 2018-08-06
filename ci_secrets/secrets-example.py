# In order to run the code, you have to provide a file named 'secrets.py' in this directory.
# This file should contain all secret variables needed by the script (passwords, etc.).
# This file is an example of how the secrets.py file should be formatted.

# Database connection
DB_host = 'localhost'
DB_port = '5432'
DB_user = 'postgres'
DB_database = 'mydb'
DB_password = 'password'

# Git repositories
GIT_base_path = '/path/to/my/repos'
GIT_token = 'f-JzjmRRnxzwqC5o3zsQ'


# Geoserver layers
GEO_base_path = '/path/to/my/layers'
GEO_url = 'localhost'
GEO_port = '8080'
GEO_number_of_pyarmid_levels = 4
GEO_user = 'admin'
GEO_password = 'geoserver'

# Taiga API
TAIGA_token = 'TOKEN_str'

# Config
SERVER='DEV'

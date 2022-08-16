Search.setIndex({"docnames": ["api/locopy", "api/locopy.database", "api/locopy.errors", "api/locopy.logger", "api/locopy.redshift", "api/locopy.s3", "api/locopy.snowflake", "api/locopy.utility", "api/modules", "developer", "examples", "index", "recipes", "snowflake", "sql_injection"], "filenames": ["api/locopy.rst", "api/locopy.database.rst", "api/locopy.errors.rst", "api/locopy.logger.rst", "api/locopy.redshift.rst", "api/locopy.s3.rst", "api/locopy.snowflake.rst", "api/locopy.utility.rst", "api/modules.rst", "developer.rst", "examples.rst", "index.rst", "recipes.rst", "snowflake.rst", "sql_injection.rst"], "titles": ["locopy package", "locopy.database module", "locopy.errors module", "locopy.logger module", "locopy.redshift module", "locopy.s3 module", "locopy.snowflake module", "locopy.utility module", "locopy", "Developer Instructions", "Basic Examples", "locopy: Data Load and Copy using Python", "Common Recipes", "Snowflake Examples", "SQL Injection"], "terms": {"databas": [0, 2, 4, 6, 7, 8, 9, 10, 13, 14], "error": [0, 4, 5, 6, 7, 8, 11, 12], "logger": [0, 8, 11], "redshift": [0, 7, 8, 9, 10, 11], "s3": [0, 2, 4, 6, 7, 8, 11], "snowflak": [0, 4, 8, 9, 11], "util": [0, 5, 8, 9, 11], "class": [1, 2, 4, 5, 6, 7, 11, 14], "dbapi": [1, 4, 6, 10, 11, 12, 13], "config_yaml": [1, 4, 6, 7, 10, 11, 12, 13], "none": [1, 3, 4, 5, 6, 9], "kwarg": [1, 4, 5, 6], "sourc": [1, 2, 3, 4, 5, 6, 7, 11], "base": [1, 2, 4, 5, 6, 7, 9], "object": [1, 4, 5, 6, 7], "thi": [1, 4, 5, 6, 7, 9, 11, 12, 14], "i": [1, 2, 3, 4, 5, 6, 7, 9, 11, 14], "all": [1, 2, 7, 9], "2": [1, 3, 4, 6, 12], "connector": [1, 4, 6, 11, 13], "which": [1, 3, 4, 5, 6, 7, 9, 10, 11, 12], "inherit": [1, 4, 6, 11], "function": [1, 4, 5, 6, 7, 11], "The": [1, 3, 4, 5, 6, 7, 9, 11, 12, 13, 14], "manag": [1, 4, 5, 6, 11], "connect": [1, 4, 6, 7, 9, 10, 11, 13], "handl": [1, 4, 11], "execut": [1, 4, 6, 9, 10, 11, 12, 13, 14], "queri": [1, 4, 6, 11, 12, 14], "most": [1, 9, 12, 14], "should": [1, 4, 5, 6, 7, 9, 11], "work": [1, 4, 5, 6, 9], "out": [1, 4, 9, 14], "box": 1, "minu": 1, "abstract": 1, "method": [1, 6, 14], "mai": [1, 4, 9, 10, 12], "vari": 1, "across": 1, "paramet": [1, 4, 5, 6, 7, 11, 12, 14], "option": [1, 2, 3, 4, 5, 6, 7, 9, 10, 11, 12, 13], "A": [1, 4, 6, 7, 9, 11, 12], "adapt": [1, 4, 6, 11, 14], "python": [1, 3, 4, 6, 7, 9], "db": [1, 4, 6, 7, 9, 10, 11, 13, 14], "api": [1, 4, 5, 6, 9], "0": [1, 4, 6, 7, 9], "compliant": [1, 4, 6], "psycopg2": [1, 4, 10, 11, 14], "pg8000": [1, 4, 10, 11, 12], "etc": [1, 4, 7, 11, 14], "str": [1, 3, 4, 5, 6, 7], "string": [1, 4, 5, 6, 7, 10, 13, 14], "repres": [1, 4, 5, 6, 7], "yaml": [1, 4, 6, 7, 11, 12], "file": [1, 2, 4, 5, 6, 7, 9, 10, 11, 13], "locat": [1, 4, 6, 7, 11, 12], "keyword": [1, 4, 5, 6], "argument": [1, 4, 5, 6], "It": [1, 4, 5, 6, 9, 11, 14], "worth": [1, 4, 6], "note": [1, 4, 5, 6, 9], "onli": [1, 4, 6, 9], "contain": [1, 4, 6, 9], "valid": [1, 4, 6, 7, 12], "you": [1, 4, 5, 6, 9, 10, 11, 12, 13], "plan": [1, 4, 6], "us": [1, 4, 5, 6, 7, 9, 10, 12, 13, 14], "throw": [1, 4, 6], "an": [1, 2, 4, 5, 6, 7, 11, 12], "except": [1, 2, 4, 6], "someth": [1, 4, 6, 9, 10, 13], "pass": [1, 3, 4, 6, 9, 10, 11, 13, 14], "through": [1, 4, 6, 9], "isn": [1, 4, 6], "t": [1, 4, 6, 7, 11], "type": [1, 3, 4, 5, 6, 7, 9, 14], "dictionari": [1, 4, 6, 7], "item": [1, 4, 6, 7, 11], "dict": [1, 4, 6, 7], "conn": [1, 4, 6], "instanc": [1, 4, 6, 11], "cursor": [1, 4, 6, 10, 11, 13], "rais": [1, 2, 4, 5, 6, 7], "credentialserror": [1, 2, 4, 6, 7], "credenti": [1, 2, 4, 5, 6, 9, 10, 11, 13], "ar": [1, 2, 4, 5, 6, 7, 9, 11, 12, 14], "provid": [1, 2, 4, 5, 6, 10, 11, 13], "both": [1, 13], "config": [1, 11, 12], "wa": 1, "column_nam": 1, "pull": 1, "column": [1, 4, 6, 7], "name": [1, 3, 4, 5, 6, 7, 9, 12], "descript": 1, "depend": [1, 11, 12], "could": [1, 4], "return": [1, 3, 4, 5, 6, 7, 9], "byte": 1, "b": [1, 6], "list": [1, 4, 5, 6, 7, 10, 13], "lower": 1, "case": [1, 4, 12], "creat": [1, 4, 6, 9, 10, 11, 12, 13], "set": [1, 3, 4, 5, 6, 7, 9, 11, 12], "valu": [1, 4, 6], "attribut": [1, 4, 6, 7], "dberror": [1, 2, 4, 6], "If": [1, 4, 5, 6, 7, 10, 11, 12, 13], "problem": [1, 4, 6, 14], "establish": [1, 4, 6], "disconnect": [1, 9], "termin": 1, "close": 1, "from": [1, 4, 5, 6, 7, 9, 10, 11, 13, 14], "sql": [1, 7, 11], "commit": [1, 11], "true": [1, 4, 6, 7, 9, 12, 13], "param": [1, 3], "mani": 1, "fals": [1, 4, 6, 10, 12], "verbos": [1, 4], "some": [1, 7, 10, 11], "against": 1, "run": [1, 4, 6, 11, 12], "one": [1, 7, 9, 11], "multipl": [1, 2, 4, 5, 6, 11, 12], "statement": [1, 4, 5, 7, 14], "boolean": [1, 4, 6, 7], "default": [1, 3, 4, 5, 6, 7, 9, 10, 12], "whether": [1, 4, 6], "command": [1, 4, 6, 9, 11, 12, 14], "cluster": [1, 4, 6, 7, 9, 10, 11, 12], "immedi": 1, "iter": [1, 6], "submit": 1, "exact": [1, 5], "syntax": 1, "bool": [1, 4, 6, 7], "script": [1, 11], "print": [1, 4, 10, 11, 13], "occur": [1, 12], "cannot": [1, 4, 14], "made": 1, "to_datafram": [1, 6, 11], "size": [1, 6, 12], "datafram": [1, 4, 6, 7], "last": [1, 6, 12], "result": [1, 6], "import": [1, 10, 11, 12, 13, 14], "panda": [1, 4, 6, 7, 9], "here": [1, 9, 10, 11], "so": [1, 12, 14], "": [1, 6, 14], "need": [1, 4, 6, 9, 11, 12], "other": [1, 7, 9, 10], "just": [1, 4, 6, 9], "conveni": [1, 6], "int": [1, 3, 4, 6, 7], "chunk": [1, 6], "fetch": [1, 6], "lowercas": [1, 6], "to_dict": 1, "gener": [1, 4, 5, 6, 11], "row": [1, 7], "yield": 1, "each": [1, 4, 7, 9, 12], "encod": 1, "compressionerror": 2, "locopyerror": 2, "when": [2, 4, 5, 6, 7, 9, 10, 12, 14], "compress": [2, 4, 6, 7, 10, 11], "user": [2, 4, 6, 7, 9, 10, 11, 13], "locopyconcaterror": [2, 7], "concaten": [2, 4, 7, 14], "baseclass": 2, "locopyignoreheadererror": [2, 7], "ignorehead": [2, 7], "found": [2, 7, 11], "copi": [2, 4, 5, 6, 7, 12, 14], "locopyspliterror": [2, 7], "split": [2, 4, 5, 7, 10, 11], "s3credentialserror": [2, 4, 5, 6], "s3error": [2, 4, 5, 6], "aw": [2, 4, 5, 6, 9, 11, 12], "s3deletionerror": [2, 5], "delet": [2, 4, 5, 6, 12, 13], "s3downloaderror": [2, 5], "download": [2, 5, 6, 11, 12], "s3initializationerror": [2, 4, 5, 6], "initi": [2, 4, 5, 6], "client": [2, 4, 5, 6, 14], "s3uploaderror": [2, 5], "upload": [2, 4, 5, 6, 9, 11, 12], "log": 3, "setsup": 3, "basic": [3, 11], "infrustrcutur": 3, "applic": [3, 7, 14], "get_logg": 3, "log_level": 3, "10": [3, 6, 11, 12], "featur": [3, 9], "level": 3, "info": [3, 4, 5, 6], "obejct": 3, "paramt": 3, "pleas": [3, 4, 5, 7, 11, 14], "see": [3, 4, 7, 11, 13], "follow": [3, 4, 5, 7, 9, 10, 11, 12, 13], "more": [3, 6, 7, 9, 11], "detail": [3, 9, 10, 11, 12, 13], "http": [3, 4, 7, 9, 11], "doc": [3, 4, 9, 11], "org": [3, 7, 9], "librari": [3, 5, 11], "html": [3, 4, 7, 9, 11], "wrap": [4, 5, 6], "can": [4, 5, 6, 9, 10, 11, 12, 13], "arbitrari": [4, 6], "code": [4, 6, 9, 10, 11, 12, 13], "profil": [4, 5, 6, 9, 10, 11, 12, 13], "kms_kei": [4, 5, 6], "implement": [4, 6], "specif": [4, 5, 6], "unload": [4, 6, 9, 11, 12, 14], "ani": [4, 7, 9, 11], "host": [4, 7, 9, 10, 11], "port": [4, 7, 9, 10, 11], "dbname": [4, 7], "password": [4, 7, 9, 10, 11, 13], "must": [4, 5, 6, 7, 9], "those": 4, "ssl": 4, "alwai": [4, 12], "enforc": 4, "typic": [4, 5, 6], "store": [4, 5, 6, 9, 11, 12], "also": [4, 5, 6, 9, 10, 11, 12], "environ": [4, 5, 6, 9, 11], "variabl": [4, 5, 6, 11], "aws_default_profil": [4, 5, 6, 11], "would": [4, 5, 6, 10, 11, 13], "instead": [4, 5, 6], "km": [4, 5, 6], "kei": [4, 5, 6], "encrypt": [4, 5, 6], "aes256": [4, 5, 6], "serversideencrypt": [4, 5, 6], "authent": [4, 5, 6, 12], "session": [4, 5, 6], "hold": [4, 5, 6], "boto3": [4, 5, 6, 7, 9], "botocor": [4, 5, 6], "ex": [4, 5, 6], "invalid": [4, 5, 6], "issu": [4, 5, 6, 7], "table_nam": [4, 6, 10, 11, 12], "s3path": 4, "delim": [4, 10, 11, 12], "copy_opt": [4, 6, 10, 13], "load": [4, 5, 6, 9, 10, 13], "tabl": [4, 6, 9, 10, 14], "being": [4, 6, 9, 12], "path": [4, 5, 6, 7], "input": [4, 7, 11, 14], "eg": 4, "csv": [4, 6, 10, 11, 13], "non": [4, 14], "delimit": [4, 7, 12], "Will": 4, "have": [4, 9, 10, 11, 13], "ad": [4, 9, 13], "ha": [4, 6, 9, 12], "been": [4, 6, 11, 13], "init": [4, 6], "wrong": 4, "insert_dataframe_to_t": [4, 6], "metadata": [4, 6], "batch_siz": 4, "1000": 4, "insert": [4, 6], "exist": [4, 6], "new": [4, 6, 9, 12], "executemani": 4, "veri": [4, 12], "poor": 4, "perform": [4, 7, 12], "term": 4, "speed": 4, "To": [4, 9, 11, 12, 13], "overcom": 4, "we": [4, 6, 9, 10, 11, 13], "format": [4, 6], "flag": [4, 6], "data": [4, 6, 7, 10, 14], "number": [4, 6, 7, 9, 12], "record": 4, "batch": 4, "load_and_copi": [4, 10, 11, 12], "local_fil": [4, 10, 11, 12], "s3_bucket": [4, 10, 11, 12], "delete_s3_aft": [4, 12], "1": [4, 6, 7, 9, 14], "s3_folder": [4, 12], "singl": [4, 6, 9, 12], "gzip": [4, 6, 7, 12], "bucket": [4, 5, 11, 12], "folder": [4, 5, 9, 12], "within": [4, 7], "your": [4, 6, 9, 10, 11, 12, 13, 14], "awar": [4, 14], "special": 4, "char": 4, "backward": 4, "slash": 4, "These": [4, 13, 14], "caus": 4, "fail": 4, "By": [4, 11, 12], "order": [4, 9, 12], "reduc": 4, "complex": 4, "critic": 4, "ensur": [4, 9], "want": [4, 9, 10, 11], "In": [4, 6, 9, 11], "For": [4, 7, 9, 13], "uniqu": [4, 12], "enough": 4, "extens": [4, 5], "get": [4, 6, 11, 12], "stripe": 4, "favour": [4, 6], "prefix": [4, 5, 12], "local": [4, 5, 6, 9, 11], "wish": [4, 5], "like": [4, 5, 9, 11], "parquet": [4, 6], "append": [4, 7], "dateformat": [4, 10], "compupd": [4, 10], "truncatecolumn": [4, 10], "thei": [4, 6, 7, 9, 11, 13], "part": [4, 5], "amazon": [4, 11], "com": [4, 7, 9, 10, 11], "latest": [4, 7, 11], "dg": 4, "convers": 4, "let": [4, 9, 12], "specifi": [4, 6, 10, 12], "after": [4, 9, 12, 13], "transfer": 4, "paralel": 4, "greater": [4, 7, 12], "than": [4, 7, 11, 12], "recommend": [4, 11], "less": [4, 7], "100": 4, "output": [4, 5, 7, 9, 12], "leav": [4, 12], "raw": 4, "convent": [4, 5], "subfold": [4, 5, 9], "unload_opt": 4, "export": [4, 6, 9, 10, 11, 13], "unload_and_copi": [4, 11, 12], "raw_unload_path": 4, "export_path": [4, 11, 12], "parallel_off": [4, 12], "write": [4, 7, 12], "flat": [4, 11], "select": [4, 9, 10, 11, 12, 13, 14], "where": [4, 5, 6, 9, 11, 14], "current": [4, 5, 6], "directori": [4, 5, 6, 9], "o": [4, 5, 6], "getcwd": [4, 5, 6], "larg": [4, 6, 12], "comma": 4, "ignor": 4, "Not": 4, "decreas": 4, "retriev": 4, "add_default_copy_opt": 4, "add": [4, 11], "job": [4, 11], "unless": 4, "request": 4, "combine_copy_opt": 4, "space": [4, 6, 13], "between": [4, 6], "convert": [4, 6], "inbetween": [4, 6], "usag": [5, 7, 14], "multipart": 5, "wrapper": 5, "push": [5, 9], "delete_from_s3": 5, "delete_list_from_s3": 5, "s3_list": 5, "includ": [5, 6, 7, 9, 11, 12], "scheme": 5, "download_from_s3": 5, "download_list_from_s3": 5, "local_path": 5, "defualt": [5, 6], "parse_s3_url": 5, "s3_url": 5, "pars": 5, "url": [5, 7, 9], "extract": 5, "disgard": 5, "upload_list_to_s3": 5, "local_list": 5, "were": 5, "NOT": 5, "look": [5, 9], "my": [5, 7, 9, 10, 11, 13], "key1": 5, "key2": 5, "There": [5, 6, 11, 12], "assumpt": 5, "via": [5, 6, 9, 10, 11, 13], "structur": 5, "file_nam": 5, "allow": [5, 13], "v": [5, 10, 13, 14], "help": [5, 9], "process": [5, 7, 9, 11, 12, 13, 14], "downstream": 5, "upload_to_s3": 5, "INTO": [6, 11], "stage": [6, 11], "file_typ": [6, 13], "format_opt": [6, 13], "field_delimit": [6, 13], "skip_head": 6, "fulli": 6, "qualifi": 6, "namespac": [6, 13], "intern": [6, 11, 14], "extern": [6, 14], "One": 6, "json": 6, "c": [6, 9], "d": [6, 11], "download_from_intern": 6, "parallel": 6, "otherwis": 6, "absolut": 6, "thread": 6, "newer": 6, "version": [6, 9], "v2": 6, "call": [6, 11, 14], "write_panda": 6, "directli": [6, 11], "custom": [6, 7, 13], "doe": 6, "build": [6, 9, 14], "tupl": 6, "doesn": [6, 7], "own": [6, 12], "significantli": 6, "appropri": [6, 12], "overrid": [6, 10], "built": 6, "fetch_pandas_al": 6, "continu": 6, "header": 6, "upload_to_intern": [6, 13], "4": 6, "auto_compress": 6, "overwrit": 6, "put": 6, "wildcard": 6, "charact": 6, "support": [6, 11], "enabl": 6, "dure": 6, "alreadi": 6, "same": 6, "overwritten": 6, "combine_opt": 6, "empti": 6, "progresspercentag": 7, "filenam": [7, 12], "s3transfer": 7, "upload_fil": 7, "callback": 7, "inform": [7, 9, 11], "readthedoc": 7, "en": 7, "refer": 7, "ref": 7, "compress_fil": 7, "input_fil": 7, "output_fil": 7, "compress_file_list": 7, "file_list": 7, "clean": [7, 9], "up": [7, 9, 11, 12], "old": 7, "origin": [7, 9, 12], "gz": [7, 9, 13], "concatenate_fil": 7, "input_list": 7, "remov": 7, "while": 7, "find_column_typ": 7, "find": 7, "check": [7, 13], "map": 7, "datetime64": 7, "n": [7, 11], "timestamp": 7, "m8": 7, "float": 7, "datetim": 7, "varchar": [7, 10, 11, 13], "get_ignoreheader_numb": 7, "number_row": 7, "AS": [7, 10], "read_config_yaml": 7, "read": [7, 11, 12], "configur": [7, 9, 11], "popul": [7, 11], "requir": [7, 11, 12], "ones": 7, "exampl": [7, 11, 12], "5439": [7, 9, 10, 11], "userid": [7, 10, 11, 13], "pointer": 7, "open": [7, 11], "miss": 7, "split_fil": 7, "ignore_head": 7, "equal": [7, 12], "line": [7, 11], "myinputfil": 7, "txt": [7, 9, 12], "myoutputfil": 7, "01": 7, "02": 7, "zero": 7, "begin": 7, "write_fil": 7, "filepath": 7, "mode": 7, "w": 7, "separ": 7, "www": 7, "tutorialspoint": 7, "python_files_io": 7, "htm": 7, "packag": [8, 9, 11], "submodul": [8, 11], "modul": [8, 11], "content": [8, 12], "guidanc": 9, "excel": 9, "sever": 9, "chang": 9, "befor": [9, 11], "dev": 9, "extra": [9, 11], "instal": 9, "ll": 9, "onc": [9, 11], "per": [9, 10], "reason": [9, 12], "behind": 9, "black": 9, "isort": 9, "machin": 9, "make": [9, 12], "style": 9, "decis": 9, "collect": 9, "wisdom": 9, "commun": 9, "pip": [9, 11], "e": [9, 12], "root": 9, "sphinx": 9, "automat": [9, 12], "regener": 9, "apidoc": 9, "render": 9, "github": [9, 11], "page": [9, 11], "sit": 9, "gh": 9, "branch": 9, "ghpage": 9, "git": 9, "about": 9, "numpi": 9, "googl": 9, "docstr": 9, "activ": [9, 11], "sure": [9, 11], "3": [9, 11], "abov": [9, 10, 12, 13], "coverag": 9, "unittest": 9, "defin": 9, "py": 9, "pytest": 9, "runner": 9, "not_integr": 9, "token": [9, 12], "locopyrc": 9, "locpi": 9, "sfrc": 9, "usernam": 9, "my_aws_profil": 9, "account": [9, 13], "warehous": [9, 13], "schema": [9, 10, 11, 12], "try": 9, "themselv": 9, "them": [9, 11], "without": [9, 11], "know": 9, "what": [9, 12], "re": [9, 10, 11, 13], "do": [9, 10, 11, 12, 13, 14], "caveat": 9, "emptor": 9, "project": [9, 11], "qualiti": 9, "setup": [9, 12], "extras_requir": 9, "keep": [9, 14], "date": 9, "subset": 9, "still": 9, "cfg": 9, "how": [9, 11], "autom": 9, "action": 9, "futur": 9, "addit": [9, 11, 12, 13], "come": [9, 14], "soon": 9, "r": 9, "updat": 9, "upgrad": [9, 11], "core": 9, "21": 9, "7": 9, "5": 9, "pyyaml": 9, "6": [9, 12], "No": 9, "pep": 9, "517": 9, "locopi": [9, 10, 12, 13, 14], "simpl": 9, "workflow": 9, "semant": 9, "peopl": [9, 12, 14], "dai": 9, "squash": 9, "merg": 9, "prevent": [9, 12], "pollut": 9, "endless": 9, "messag": 9, "collaps": 9, "much": 9, "easier": 9, "back": [9, 12], "break": 9, "master": 9, "offici": 9, "go": [9, 12], "tag": 9, "properli": 9, "denot": 9, "correspond": 9, "artifact": 9, "first": 9, "repo": [9, 11], "orphan": 9, "sinc": 9, "independ": 9, "checkout": 9, "makefil": 9, "helper": 9, "streamlin": 9, "right": 9, "below": 9, "taken": 9, "setuptool": [9, 11], "wheel": 9, "twine": 9, "sdist": 9, "bdist_wheel": 9, "under": 9, "dist": 9, "py3": 9, "whl": 9, "tar": 9, "final": 9, "repositori": 9, "legaci": 9, "real": 9, "create_sql": [10, 13], "20": [10, 11, 13], "distkei": [10, 11], "aws_profil": [10, 13], "example_data": [10, 11, 13], "my_s3_bucket": [10, 11], "fetchal": [10, 11, 13], "ident": 10, "explicitli": [10, 13], "constructor": [10, 11, 13], "rather": [10, 11, 13], "aws_access_key_id": [10, 11, 13], "my_access_key_id": [10, 13], "aws_secret_access_kei": [10, 11, 13], "my_secret_access_kei": [10, 13], "aws_session_token": [10, 13], "my_session_token": [10, 13], "As": [10, 11], "document": [10, 11, 13], "time": 10, "tweak": [10, 12], "odditi": 10, "assign": 10, "few": [10, 11], "auto": 10, "ON": 10, "three": 10, "null": 10, "escap": 10, "assist": 11, "etl": [11, 14], "8": 11, "driver": 11, "agnost": 11, "favourit": 11, "compli": 11, "conda": 11, "forg": 11, "channel": 11, "virtual": 11, "highli": 11, "virtualenv": 11, "bin": 11, "postgr": 11, "prefer": 11, "end": 11, "test": 11, "ever": 11, "consist": 11, "sslmode": 11, "another_opt": 11, "123": 11, "aren": 11, "don": 11, "yml": [11, 12], "df": 11, "query_group": 11, "TO": 11, "my_profil": [11, 12], "some_profile_with_valid_token": [11, 12], "my_output_destin": 11, "abl": 11, "assum": [11, 12], "iam": 11, "role": 11, "ec2": 11, "access": [11, 12], "interfac": 11, "cli": 11, "userguid": 11, "chap": 11, "start": [11, 12], "preced": 11, "leverag": 11, "either": 11, "associ": 11, "attach": 11, "welcom": 11, "appreci": 11, "contribut": 11, "accept": [11, 13], "ask": 11, "sign": 11, "licens": 11, "agreement": 11, "cla": 11, "adher": 11, "conduct": 11, "particip": 11, "expect": 11, "honor": 11, "common": 11, "recip": 11, "backup": 11, "inject": 11, "develop": 11, "pre": 11, "hook": 11, "unit": 11, "integr": 11, "edgetest": 11, "releas": 11, "guid": 11, "distribut": 11, "archiv": 11, "pypi": 11, "index": 11, "search": 11, "over": 12, "interact": [12, 14], "probabl": 12, "coupl": 12, "wai": 12, "regular": 12, "basi": 12, "might": [12, 14], "sens": 12, "secur": 12, "place": 12, "resourc": 12, "some_data_to_load": 12, "s3_bucket_to_us": 12, "s3_folder_to_us": 12, "s3_subfolder_to_us": 12, "redshift_table_to_load": 12, "take": 12, "integ": 12, "realli": 12, "ideal": 12, "slice": 12, "avail": 12, "turn": 12, "off": 12, "combin": 12, "drastic": 12, "alter": 12, "suppli": 12, "Or": 12, "pipe": 12, "tsv": 12, "previous": 12, "older": 12, "differ": 12, "omit": 12, "unique_file_prefix": 12, "simpli": 12, "noth": 12, "els": 12, "maximum": 12, "gb": 12, "sf": 13, "internal_stag": 13, "similar": 13, "trim_spac": 13, "forc": 13, "purg": 13, "trim": 13, "successfulli": 13, "full": 13, "topic": 14, "deal": 14, "best": 14, "practic": 14, "involv": 14, "id": 14, "cur": 14, "interpol": 14, "usual": 14, "though": 14, "plai": 14, "nice": 14, "unfortun": 14, "mind": 14, "limit": 14, "realiz": 14, "face": 14, "think": 14, "websit": 14, "field": 14, "idea": 14, "known": 14, "safer": 14}, "objects": {"": [[0, 0, 0, "-", "locopy"]], "locopy": [[1, 0, 0, "-", "database"], [2, 0, 0, "-", "errors"], [3, 0, 0, "-", "logger"], [4, 0, 0, "-", "redshift"], [5, 0, 0, "-", "s3"], [6, 0, 0, "-", "snowflake"], [7, 0, 0, "-", "utility"]], "locopy.database": [[1, 1, 1, "", "Database"]], "locopy.database.Database": [[1, 2, 1, "", "column_names"], [1, 3, 1, "", "conn"], [1, 2, 1, "", "connect"], [1, 3, 1, "", "connection"], [1, 3, 1, "", "cursor"], [1, 3, 1, "", "dbapi"], [1, 2, 1, "", "disconnect"], [1, 2, 1, "", "execute"], [1, 2, 1, "", "to_dataframe"], [1, 2, 1, "", "to_dict"]], "locopy.errors": [[2, 4, 1, "", "CompressionError"], [2, 4, 1, "", "CredentialsError"], [2, 4, 1, "", "DBError"], [2, 4, 1, "", "LocopyConcatError"], [2, 4, 1, "", "LocopyError"], [2, 4, 1, "", "LocopyIgnoreHeaderError"], [2, 4, 1, "", "LocopySplitError"], [2, 4, 1, "", "S3CredentialsError"], [2, 4, 1, "", "S3DeletionError"], [2, 4, 1, "", "S3DownloadError"], [2, 4, 1, "", "S3Error"], [2, 4, 1, "", "S3InitializationError"], [2, 4, 1, "", "S3UploadError"]], "locopy.logger": [[3, 5, 1, "", "get_logger"]], "locopy.redshift": [[4, 1, 1, "", "Redshift"], [4, 5, 1, "", "add_default_copy_options"], [4, 5, 1, "", "combine_copy_options"]], "locopy.redshift.Redshift": [[4, 3, 1, "", "conn"], [4, 2, 1, "", "connect"], [4, 3, 1, "", "connection"], [4, 2, 1, "", "copy"], [4, 3, 1, "", "cursor"], [4, 3, 1, "", "dbapi"], [4, 2, 1, "", "insert_dataframe_to_table"], [4, 3, 1, "", "kms_key"], [4, 2, 1, "", "load_and_copy"], [4, 3, 1, "", "profile"], [4, 3, 1, "", "s3"], [4, 3, 1, "", "session"], [4, 2, 1, "", "unload"], [4, 2, 1, "", "unload_and_copy"]], "locopy.s3": [[5, 1, 1, "", "S3"]], "locopy.s3.S3": [[5, 2, 1, "", "delete_from_s3"], [5, 2, 1, "", "delete_list_from_s3"], [5, 2, 1, "", "download_from_s3"], [5, 2, 1, "", "download_list_from_s3"], [5, 3, 1, "", "kms_key"], [5, 2, 1, "", "parse_s3_url"], [5, 3, 1, "", "profile"], [5, 3, 1, "", "s3"], [5, 3, 1, "", "session"], [5, 2, 1, "", "upload_list_to_s3"], [5, 2, 1, "", "upload_to_s3"]], "locopy.snowflake": [[6, 1, 1, "", "Snowflake"], [6, 5, 1, "", "combine_options"]], "locopy.snowflake.Snowflake": [[6, 3, 1, "", "conn"], [6, 2, 1, "", "connect"], [6, 3, 1, "", "connection"], [6, 2, 1, "", "copy"], [6, 3, 1, "", "cursor"], [6, 3, 1, "", "dbapi"], [6, 2, 1, "", "download_from_internal"], [6, 2, 1, "", "insert_dataframe_to_table"], [6, 3, 1, "", "kms_key"], [6, 3, 1, "", "profile"], [6, 3, 1, "", "s3"], [6, 3, 1, "", "session"], [6, 2, 1, "", "to_dataframe"], [6, 2, 1, "", "unload"], [6, 2, 1, "", "upload_to_internal"]], "locopy.utility": [[7, 1, 1, "", "ProgressPercentage"], [7, 5, 1, "", "compress_file"], [7, 5, 1, "", "compress_file_list"], [7, 5, 1, "", "concatenate_files"], [7, 5, 1, "", "find_column_type"], [7, 5, 1, "", "get_ignoreheader_number"], [7, 5, 1, "", "read_config_yaml"], [7, 5, 1, "", "split_file"], [7, 5, 1, "", "write_file"]]}, "objtypes": {"0": "py:module", "1": "py:class", "2": "py:method", "3": "py:attribute", "4": "py:exception", "5": "py:function"}, "objnames": {"0": ["py", "module", "Python module"], "1": ["py", "class", "Python class"], "2": ["py", "method", "Python method"], "3": ["py", "attribute", "Python attribute"], "4": ["py", "exception", "Python exception"], "5": ["py", "function", "Python function"]}, "titleterms": {"locopi": [0, 1, 2, 3, 4, 5, 6, 7, 8, 11], "packag": 0, "submodul": 0, "modul": [0, 1, 2, 3, 4, 5, 6, 7], "content": [0, 11], "databas": [1, 11], "error": 2, "logger": 3, "redshift": [4, 12], "s3": [5, 10, 12], "snowflak": [6, 13], "util": 7, "develop": 9, "instruct": [9, 11], "pre": 9, "commit": 9, "hook": 9, "gener": 9, "document": 9, "run": [9, 10, 13], "unit": 9, "test": 9, "integr": 9, "manag": 9, "requir": 9, "edgetest": 9, "releas": 9, "guid": 9, "distribut": 9, "archiv": 9, "pypi": 9, "basic": 10, "exampl": [10, 13], "upload": [10, 13], "copi": [10, 11, 13], "command": [10, 13], "yaml": [10, 13], "without": [10, 13], "aw": [10, 13], "token": [10, 11, 13], "environ": [10, 13], "variabl": [10, 13], "extra": [10, 13], "paramet": [10, 13], "job": [10, 13], "data": [11, 12], "load": [11, 12], "us": 11, "python": 11, "quick": 11, "instal": 11, "api": 11, "specif": 11, "2": 11, "0": 11, "usag": 11, "note": 11, "advanc": 11, "contributor": 11, "roadmap": 11, "refer": 11, "indic": 11, "tabl": [11, 12, 13], "common": 12, "recip": 12, "i": 12, "have": 12, "file": 12, "want": 12, "d": 12, "like": 12, "split": 12, "my": 12, "n": 12, "don": 12, "t": 12, "compress": 12, "export": 12, "some": 12, "from": 12, "local": 12, "csv": 12, "backup": 12, "intern": 13, "stage": 13, "sql": 14, "inject": 14}, "envversion": {"sphinx.domains.c": 2, "sphinx.domains.changeset": 1, "sphinx.domains.citation": 1, "sphinx.domains.cpp": 6, "sphinx.domains.index": 1, "sphinx.domains.javascript": 2, "sphinx.domains.math": 2, "sphinx.domains.python": 3, "sphinx.domains.rst": 2, "sphinx.domains.std": 2, "sphinx.ext.intersphinx": 1, "sphinx.ext.todo": 2, "sphinx.ext.viewcode": 1, "sphinx": 56}})
import os

from pyhocon import ConfigFactory


def read_conf(conf_path, env):
    """Read the pipeline conf, filling in ${env} and ${CONF_HOME} in the include lines.
    Mirrors ConfigParser + CustomConfigIncluder."""
    conf_home = os.environ.get("CONF_HOME", os.path.dirname(os.path.abspath(conf_path)))
    conf_home = conf_home.replace("\\", "/")
    with open(conf_path, encoding="utf-8") as f:
        text = f.read()
    text = text.replace("${env}", env).replace("${CONF_HOME}", conf_home)
    return ConfigFactory.parse_string(text)
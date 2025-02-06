from .__version__ import __title__
from .cli import app

if __name__ == "__main__":
    app(prog_name=__title__)

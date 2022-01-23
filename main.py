
import click

from app import app
from etl import ETL


#############################################################################
# Click option
#############################################################################
@click.group()
def cli():
    pass


@cli.command()
def run_app():
    print("running app")
    app.run("0.0.0.0", debug=True)


@cli.command()
def create_tables():
    etl = ETL()
    etl.main()


if __name__ == '__main__':
    cli()


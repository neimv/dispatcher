
import click
from flask import Flask
from flask_restful import Resource, Api


#############################################################################
# App flask
#############################################################################
app = Flask(__name__)
api = Api(app)


class HelloWorld(Resource):
    def get(self):
        return {'hello': 'world'}


api.add_resource(HelloWorld, '/')


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


if __name__ == '__main__':
    cli()


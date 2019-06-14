""" Development tasks to simplify working with packages and docker """
from invoke import task


@task
def install(ctx):
    """ Install the package using dev dependencies and linked source """
    ctx.run("python3 -m pip install -e .[dev]")


@task
def run(ctx, remove=True):
    """ Run the kafka-to-hbase command inside a properly configured docker container """
    rm = '--rm' if remove else ''
    ctx.run(f"docker-compose run {rm} kafka-to-hbase")


@task
def build(ctx):
    """ Build the docker images used in development """
    ctx.run("docker-compose build")


@task
def up(ctx):
    """ Bring up the development stack including Kafka and Hbase """
    ctx.run("docker-compose up -d")


@task
def down(ctx):
    """ Bring down the development environment """
    ctx.run("docker-compose down")


@task
def cleanup(ctx):
    """ Remove old containers and clean up """
    ctx.run("docker-compose rm -fv")
    ctx.run("docker volume prune -f")
    ctx.run("docker network prune -f")

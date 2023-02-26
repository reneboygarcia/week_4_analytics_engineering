# Week 4 | Homework
# import
from prefect.filesystems import GitHub
from prefect.deployments import Deployment
from prefect.infrastructure.docker import DockerContainer
from etl_web_to_bq_wk4 import parent_etl_web_to_bq

# Fetch storage from GitHub
github_block = GitHub.load("ny-taxi-github-block-wk4")
# Set-up Infrastructure
docker_block = DockerContainer.load("prefect-docker-block")


# https://docs.prefect.io/api-ref/prefect/deployments/#prefect.deployments.Deployment.build_from_flow

bq_git_dep = Deployment.build_from_flow(
    flow=parent_etl_web_to_bq,
    name="ny-taxi-flow-wk4",
    storage=github_block,
)

print("Successfully deployed NY Taxi Github Block with Docker. Check app.prefect.cloud")

# execute
if __name__ == "__main__":
    bq_git_dep.apply()

# to deploy
# --params '{"year":2019, "months": [2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 1], "colors": ["yellow", "green"]}'
# prefect deployment run parent-etl-web-to-bq/ny-taxi-flow-wk4 --params '{"year":2020, "months": [4], "colors": ["yellow", "green"]}'

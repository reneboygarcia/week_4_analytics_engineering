# Week 3 | Homework
# from prefect orion
# import
from prefect.filesystems import GitHub
from prefect.deployments import Deployment
from etl_web_gcs_hw_wk4_fhv import etl_parent_web_gcs

# Load GitHub Block
github_block = GitHub.load("ny-taxi-github-block-wk4")

# https://docs.prefect.io/api-ref/prefect/deployments/#prefect.deployments.Deployment.build_from_flow

gsc_git_dep = Deployment.build_from_flow(
    flow=etl_parent_web_gcs,
    name="ny-taxi-flow-gcs-hw-wk4",
    storage=github_block,
)

print("Successfully created a Deployment. Check app.prefect.cloud")

# execute
if __name__ == "__main__":
    gsc_git_dep.apply()

# to deploy
# prefect deployment run etl-parent-web-gcs/ny-taxi-flow-gcs-hw-wk4 --params '{"years":[2020], "months": [2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 1]}'
# prefect deployment run etl-parent-web-gcs/ny-taxi-flow-gcs-hw-wk4 --params '{"years":[2020], "months": [6, 7, 8, 9, 10, 11, 12, 1]}'

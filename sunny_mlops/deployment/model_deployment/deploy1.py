import sys
import pathlib
import mlflow
from mlflow.tracking import MlflowClient
from mlflow.deployments import get_deploy_client
import time

sys.path.append(str(pathlib.Path(__file__).parent.parent.parent.resolve()))




def deploy(model_uri, env):
    """Deploys an already-registered model in Unity catalog by assigning it the appropriate alias for model deployment.

    :param model_uri: URI of the model to deploy. Must be in the format "models:/<name>/<version-id>", as described in
                      https://www.mlflow.org/docs/latest/model-registry.html#fetching-an-mlflow-model-from-the-model-registry
    :param env: name of the environment in which we're performing deployment, i.e one of "dev", "staging", "prod".
                Defaults to "dev"
    :return:
    """
    # print(f"Deployment running in env: {env}")
    # _, model_name, version = model_uri.split("/")
    # client = MlflowClient(registry_uri="databricks-uc")
    # mv = client.get_model_version(model_name, version)
    # target_alias = "champion"
    # if target_alias not in mv.aliases:
    #     client.set_registered_model_alias(
    #         name=model_name,
    #         alias=target_alias, 
    #         version=version)
    #     print(f"Assigned alias '{target_alias}' to model version {model_uri}.")
        
    #     # remove "challenger" alias if assigning "champion" alias
    #     if target_alias == "champion" and "challenger" in mv.aliases:
    #         print(f"Removing 'challenger' alias from model version {model_uri}.")
    #         client.delete_registered_model_alias(
    #             name=model_name,
    #             alias="challenger")

    import mlflow
    print(f"Deployment running in env: {env}")
    _, model_name, version = model_uri.split("/")
    client = MlflowClient(registry_uri="databricks-uc")
    # deploy_client = mlflow.deployments.get_deploy_client("databricks")
    deploy_client = get_deploy_client("databricks")
    mv = client.get_model_version(model_name, version)
    target_alias = "champion"
    if target_alias not in mv.aliases:
        client.set_registered_model_alias(
            name=model_name,
            alias=target_alias, 
            version=version)
        print(f"Assigned alias '{target_alias}' to model version {model_uri}.")
        
        # remove "challenger" alias if assigning "champion" alias
        if target_alias == "champion" and "challenger" in mv.aliases:
            print(f"Removing 'challenger' alias from model version {model_uri}.")
            client.delete_registered_model_alias(
                name=model_name,
                alias="challenger")
            
    model_version_info = client.get_model_version_by_alias(model_name, target_alias)

    # Convert the model version to string
    model_version = str(model_version_info.version)

    endpoint_name = f"{model_name.split('.')[-1]}-endpoint"


    try:
        deploy_client.get_endpoint(endpoint=endpoint_name)
        print(f"Model endpoint {endpoint_name} exists. Updating endpoint.")

        deploy_client.update_endpoint(endpoint=endpoint_name,
                    config={
                        "served_entities": [
                            {
                                "name": endpoint_name,
                                "entity_name": f"{model_name}",
                                "entity_version": model_version,  # Use the previously retrieved model version
                                "workload_size": "Small",
                                "scale_to_zero_enabled": True
                            }
                        ]})
        
        # Poll the endpoint status until it is ready
        while deploy_client.get_endpoint(endpoint=endpoint_name)["state"]["config_update"] in ["NOT_READY", "IN_PROGRESS"]:
            print("Waiting for endpoint to be updated.")
            time.sleep(60)
        print(f"Endpoint is updated! Model {model_name} with version {model_version} is now in production")

        client.delete_registered_model_alias(name=f"{model_name}", alias=target_alias)
        client.delete_registered_model_alias(name=f"{model_name}", alias="production")
        client.set_registered_model_alias(
                    name=f"{model_name}", alias="production", version=int(model_version))
        
    except:  
        print(f"Model endpoint {endpoint_name} does not exist. Creating endpoint.")

        endpoint = deploy_client.create_endpoint(name=endpoint_name,
                        config={
                            "served_entities": [
                                {
                                    "name": endpoint_name,
                                    "entity_name": f"{model_name}",
                                    "entity_version": model_version,  # Use the previously retrieved model version
                                    "workload_size": "Small",
                                    "scale_to_zero_enabled": True
                                }
                            ]
                        }
                    )
        
        while deploy_client.get_endpoint(endpoint=endpoint_name)["state"]["ready"] in ["NOT_READY", "IN_PROGRESS"]:
            print("Waiting for endpoint to be ready")
            time.sleep(60)


        print(f"Endpoint is ready! Model {model_name} with version {model_version} is now in production")
        # Clean up old aliases if they exist
        client.delete_registered_model_alias(name=f"{model_name}", alias=target_alias)
        # Set the new model version as the production alias
        client.set_registered_model_alias(name=f"{model_name}", alias="production", version=int(model_version))
        


if __name__ == "__main__":
    deploy(model_uri=sys.argv[1], env=sys.argv[2])

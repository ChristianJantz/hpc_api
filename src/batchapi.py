import argparse
from configparser import ConfigParser
from azure.batch import BatchServiceClient, batch_auth
from azure.storage.blob import BlobServiceClient
import azure.batch.models as batchmodels

SAMPLE_CONFIG_FILE_NAME = "configuration.cfg"
OSTYPE = {"linux", "windows"}

def print_batch_exception(batch_exception: batchmodels.BatchErrorException):
    """
    Prints the contents of the specified Batch exception.
    :param batch_exception:
    """
    print('-------------------------------------------')
    print('Exception encountered:')
    if batch_exception.error and \
            batch_exception.error.message and \
            batch_exception.error.message.value:
        print(batch_exception.error.message.value)
        if batch_exception.error.values:
            print()
            for mesg in batch_exception.error.values:
                print(f'{mesg.key}:\t{mesg.value}')
    print('-------------------------------------------')
# define args
def parse_args():
    
    parser = argparse.ArgumentParser(description="Wellcome to Azure Batch VM Pool Creation")
    # pool name
    parser.add_argument("--pool_id", type=str, help="Name of the Azure Batch Pool", required=True)
    # job name
    parser.add_argument("--job_id", type=str, help="Name of the Job in the Pool", required=True)
    # os_type
    parser.add_argument("ostype", type=str, choices=OSTYPE,help="Type of the OS for virtual Machine")
    # vm_size
    parser.add_argument("--vm_size", type=str, help="Size of the VM example standard_a1_v2", default="standard_a1_v2")
    
    return parser.parse_args()


def create_pool(batch_service_client: BatchServiceClient, blob_client: BlobServiceClient, ostype: str, pool_id, vm_size: str):
    
    mounts = [
        batchmodels.MountConfiguration(
            azure_blob_file_system_configuration=batchmodels.AzureBlobFileSystemConfiguration(
                account_key=blob_client.credential.account_key,
                account_name=blob_client.account_name,
                container_name="blobfuse",
                relative_mount_path="{}-{}".format(blob_client.account_name, "blobfuse"),
                blobfuse_options="-o allow_other -o attr_timeout=240 -o entry_timeout=240 -o negative_timeout=120"
            )
        ),
        batchmodels.MountConfiguration(
            nfs_mount_configuration=batchmodels.NFSMountConfiguration(
                source="{}.blob.core.windows.net/{}".format(blob_client.account_name, "blobfuse"),
                relative_mount_path="blobfuse",
                mount_options="-o vers=3.0,sec=sys,proto=tcp,nolock"
            )
        )
    ]
    env_config = [
        batchmodels.EnvironmentSetting(
            name="AZURE_INSIGHTS_INSTRUMENTATION_KEY",
            value=global_cfg.get(
                "insights", "app_insights_instrumentation_key")
        ),
        batchmodels.EnvironmentSetting(
            name="BATCH_INSIGHTS_DOWNLOAD_GIT_URL",
            value="https://github.com/Azure/batch-insights/releases/download/v1.3.0/batch-insights"
        ),
        batchmodels.EnvironmentSetting(
            name="APP_INSIGHTS_APP_ID",
            value=global_cfg.get("insights", "app_insights_app_id")
        )
    ]
    
    if ostype.lower() == "linux":
        new_pool = batchmodels.PoolAddParameter(
            id=pool_id,
            virtual_machine_configuration=batchmodels.VirtualMachineConfiguration(
                image_reference=batchmodels.ImageReference(
                    publisher="microsoft-azure-batch",
                    offer="ubuntu-server-container",
                    sku="20-04-lts",
                    version="latest"
                ),
                node_agent_sku_id="batch.node.ubuntu 20.04"
            ),
            vm_size= vm_size,
            target_dedicated_nodes= 2,
            start_task=batchmodels.StartTask(
                command_line="/bin/bash -c wget -o https://raw.githubusercontent.com/Azure/batch-insights/master/scripts/run-linux.sh",
                environment_settings=env_config,
                user_identity=batchmodels.UserIdentity(
                    auto_user=batchmodels.AutoUserSpecification(
                        scope=batchmodels.AutoUserScope.pool,
                        elevation_level=batchmodels.ElevationLevel.admin
                    ) 
                ),
                wait_for_success=True,
                max_task_retry_count=3,
            ),
            mount_configuration=mounts,
            network_configuration=batchmodels.NetworkConfiguration(
                subnet_id="/subscriptions/5e17ca5d-8099-458a-851c-526a4b9bd1ef/resourceGroups/az-batch-ccl-rg/providers/Microsoft.Network/virtualNetworks/vnet-westeurope/subnets/test-pool-batch",
                public_ip_address_configuration=batchmodels.PublicIPAddressConfiguration(
                    provision="NoPublicIPAddresses"
                )
            )
        )
        batch_service_client.pool.add(new_pool)
    elif ostype.lower() == "windows":
        new_pool = batchmodels.PoolAddParameter(
            id=pool_id,
            virtual_machine_configuration=batchmodels.VirtualMachineConfiguration(
                image_reference=batchmodels.ImageReference(
                    publisher="microsoftwindowsserver",
                    offer="windowsserver",
                    sku="2019-datacenter",
                    version="latest"
                ),
                node_agent_sku_id="batch.node.windows amd64"
            ),
            vm_size= vm_size,
            target_dedicated_nodes=2,
            
        )
        batch_service_client.pool.add(new_pool)
    else:
        raise ValueError("unkown OS Type: ")

def create_job_in_pool(batch_service_client: BatchServiceClient, job_id: str, pool_id: str):
    job = batchmodels.JobAddParameter(
        id=job_id,
        pool_info=batchmodels.PoolInformation(pool_id=pool_id)
    )
    
    batch_service_client.job.add(job)

if __name__ == "__main__":
    
    args = parse_args()
    
    global_cfg = ConfigParser()
    global_cfg.read(SAMPLE_CONFIG_FILE_NAME)
    
    batch_account_name = global_cfg.get("batch", "batch_account_name")
    batch_account_key = global_cfg.get("batch", "batch_account_key")
    batch_account_url = global_cfg.get("batch", "batch_account_url")

    storage_account_name = global_cfg.get("storage", "storage_account_name")
    storage_account_key = global_cfg.get("storage", "storage_account_key")
    storage_account_url = global_cfg.get("storage", "storage_blob_url")
    
    credentials = batch_auth.SharedKeyCredentials(
        account_name=batch_account_name,
        key=batch_account_key
    )
    
    batch_client = BatchServiceClient(
        credentials=credentials,
        batch_url=batch_account_url
    )
    
    blob_client = BlobServiceClient(
        account_url=storage_account_url,
        credential=storage_account_key
    )
    
    
    try:
        create_pool(batch_service_client=batch_client,blob_client=blob_client, ostype=args.ostype, pool_id=args.pool_id, vm_size=args.vm_size)
        create_job_in_pool(batch_client, args.job_id, args.pool_id)
    except batchmodels.BatchErrorException as err:
        print_batch_exception(err)
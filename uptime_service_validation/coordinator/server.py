import logging
import sys
from kubernetes import client, config
import os
from datetime import datetime, timezone
import subprocess
import time


# Format datetime such as it is accepted by the stateless validator
def datetime_formatter(dt):
    return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-5] + "+0000"


def setUpValidatorPods(time_intervals, logging, worker_image, worker_tag):
    # Configuring Kubernetes client
    config.load_incluster_config()
    # config.load_kube_config()

    api = client.BatchV1Api()
    namespace = (
        open("/var/run/secrets/kubernetes.io/serviceaccount/namespace").read().strip()
    )

    # List to keep track of job names
    jobs = []

    for index, mini_batch in enumerate(time_intervals):
        # Define the environment variables
        env_vars = [
            client.V1EnvVar(
                name="CQLSH", value=os.environ.get("CQLSH")
            ),
            client.V1EnvVar(
                name="DELEGATION_VERIFY_AWS_ROLE_ARN", value=os.environ.get("DELEGATION_VERIFY_AWS_ROLE_ARN")
            ),
            client.V1EnvVar(
                name="DELEGATION_VERIFY_AWS_ROLE_SESSION_NAME", value=os.environ.get("DELEGATION_VERIFY_AWS_ROLE_SESSION_NAME")
            ),
            client.V1EnvVar(
                name="CASSANDRA_HOST", value=os.environ.get("CASSANDRA_HOST")
            ),
            client.V1EnvVar(
                name="CASSANDRA_PORT", value=os.environ.get("CASSANDRA_PORT")
            ),
            client.V1EnvVar(
                name="AWS_DEFAULT_REGION", value=os.environ.get("AWS_DEFAULT_REGION")
            ),
            client.V1EnvVar(
                name="SSL_CERTFILE", value="/.cassandra/sf-class2-root.crt"
            ),
            client.V1EnvVar(name="CASSANDRA_USE_SSL", value="1"),
            client.V1EnvVar(
                name="AUTH_VOLUME_MOUNT_PATH", value=os.environ.get("AUTH_VOLUME_MOUNT_PATH")
            ),
        ]

        # Define the volumes
        auth_volume = client.V1Volume(
            name = "auth-volume",
            empty_dir = client.V1EmptyDirVolumeSource(),
        )

        # Define the volueMounts
        auth_volume_mount = client.V1VolumeMount(
            name = "auth-volume",
            mount_path = os.environ.get("AUTH_VOLUME_MOUNT_PATH"),
        )

        # Define the container

        container = client.V1Container(
            name="stateless-verification-tool",
            image=f"{worker_image}:{worker_tag}",
            command=["/bin/sh", "-c", "source /var/mina-delegation-verify-auth/.env && /bin/delegation-verify"],
            args=[
                "cassandra",
                "--keyspace",
                os.environ.get("AWS_KEYSPACE"),
                datetime_formatter(mini_batch[0]),
                datetime_formatter(mini_batch[1]),
                "--no-check",
            ],
            env=env_vars,
            image_pull_policy=os.environ.get("IMAGE_PULL_POLICY", "IfNotPresent"),
            volume_mounts=[auth_volume_mount],
        )

        # Define the init container

        init_container = client.V1InitContainer(
            name="stateless-verification-tool-init",
            image=f"{worker_image}:{worker_tag}",
            command=["/bin/authenticate.sh"],
            env=env_vars,
            image_pull_policy=os.environ.get("IMAGE_PULL_POLICY", "IfNotPresent"),
            volume_mounts=[auth_volume_mount],
        )

        # Job name
        job_name = f"stateless-verification-tool-{datetime.now(timezone.utc).strftime('%y-%m-%d-%H-%M')}-{index}"

        # Create the job
        job = client.V1Job(
            api_version="batch/v1",
            kind="Job",
            metadata=client.V1ObjectMeta(name=job_name),
            spec=client.V1JobSpec(
                template=client.V1PodTemplateSpec(
                    spec=client.V1PodSpec(
                        containers=[container, init_container], restart_policy="Never"
                    )
                )
            ),
        )

        # Create the job in Kubernetes
        try:
            api.create_namespaced_job(namespace, job)
            logging.info(f"Job {job_name} created in namespace {namespace}")
            jobs.append(job_name)
        except Exception as e:
            logging.error(f"Error creating job {job_name}: {e}")

    # Monitor jobs
    while jobs:
        for job_name in list(jobs):
            try:
                job_status = api.read_namespaced_job_status(job_name, namespace)
                if job_status.status.succeeded:
                    logging.info(f"Job {job_name} succeeded.")
                    jobs.remove(job_name)
                elif job_status.status.failed:
                    logging.error(f"Job {job_name} failed.")
                    jobs.remove(job_name)
            except Exception as e:
                logging.error(f"Error reading job status for {job_name}: {e}")

        time.sleep(10)

    logging.info("All jobs have been processed.")


def setUpValidatorProcesses(time_intervals, logging, worker_image, worker_tag):
    processes = []
    for index, mini_batch in enumerate(time_intervals):
        process_name = (
            f"local-validator-{datetime.now().strftime('%y-%m-%d-%H-%M')}-{index}"
        )
        image = f"{worker_image}:{worker_tag}"
        command = [
            "docker",
            "run",
            # "-it",
            "--privileged",
            "--network",
            "host",
            "--rm",
            "-v",
            f"{os.environ.get('SSL_CERTFILE')}:/var/ssl/ssl-cert.crt",
            "--env",
            "CASSANDRA_HOST",
            "--env",
            "CASSANDRA_PORT",
            "--env",
            "AWS_ACCESS_KEY_ID",
            "--env",
            "AWS_SECRET_ACCESS_KEY",
            "--env",
            "AWS_DEFAULT_REGION",
            "--env",
            "SSL_CERTFILE=/var/ssl/ssl-cert.crt",
            "--env",
            "CASSANDRA_USE_SSL=1",
            image,
            "/bin/delegation-verify",
            "cassandra",
            "--keyspace",
            os.environ.get("AWS_KEYSPACE"),
            f"{datetime_formatter(mini_batch[0])}",
            f"{datetime_formatter(mini_batch[1])}",
            "--no-check",
        ]
        # command = [
        #     "delegation-verify",
        #     "cassandra",
        #     "--keyspace",
        #     os.environ.get("AWS_KEYSPACE"),
        #     f"{datetime_formatter(mini_batch[0])}",
        #     f"{datetime_formatter(mini_batch[1])}",
        #     "--no-check",
        # ]
        cmd_str = " ".join(command)

        # Set up environment variables for the process
        env = os.environ.copy()
        env["JobName"] = process_name
        env["BatchStart"] = str(mini_batch[0])
        env["BatchEnd"] = str(mini_batch[1])

        # Spawn the process
        proc = subprocess.Popen(
            command, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        processes.append((process_name, proc))
        logging.info(f"Launching process {index}: {cmd_str}")

    # Monitor the processes
    while processes:
        for process_name, proc in processes:
            if proc.poll() is not None:  # Check if the process has completed
                # Handle process stdout and stderr and remove it
                output, errors = proc.communicate()
                logging.info(f"Process {process_name} completed at {datetime.now()}")
                if output:
                    logging.info(f"{process_name} (stdout): {output.decode().strip()}")
                if errors:
                    logging.error(f"{process_name} (stderr): {errors.decode().strip()}")

                processes.remove((process_name, proc))

        time.sleep(1)

        if not processes:
            break


# Usage Example
if __name__ == "__main__":
    logging.basicConfig(
        stream=sys.stdout,
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    time_intervals = [
        (
            datetime(2023, 11, 14, 14, 35, 47, 630),
            datetime(2023, 11, 14, 14, 36, 17, 630),
        ),
        (
            datetime(2023, 11, 14, 14, 36, 17, 630),
            datetime(2023, 11, 14, 14, 36, 47, 630),
        ),
        (
            datetime(2023, 11, 14, 14, 36, 47, 630),
            datetime(2023, 11, 14, 14, 37, 17, 630),
        ),
        (
            datetime(2023, 11, 14, 14, 37, 17, 630),
            datetime(2023, 11, 14, 14, 37, 47, 630),
        ),
        (
            datetime(2023, 11, 14, 14, 37, 47, 630),
            datetime(2023, 11, 14, 14, 38, 17, 630),
        ),
    ]
    setUpValidatorProcesses(
        time_intervals,
        logging,
        worker_image=os.environ.get("WORKER_IMAGE"),
        worker_tag=os.environ.get("WORKER_TAG"),
    )

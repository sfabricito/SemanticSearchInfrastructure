# Semantic Search Infraestructure

## Requirements

The requirements for the project are the following:

* Create an user in [DockerHub](https://hub.docker.com/)
* Install [Docker Desktop](https://docs.docker.com/desktop/install/windows-install/), if you are using MacOS, please make sure you select the right installer for your CPU architecture.
* Open Docker Desktop, go to **Settings > Kubernetes** and enable Kubernetes.
![K8s](./images/docker-desktop-k8s.png "K8s Docker Desktop")
* Install [Kubectl](https://kubernetes.io/docs/tasks/tools/install-kubectl-macos/)
* Install [Helm](https://helm.sh/docs/intro/install/)
* Install [Visual Studio Code](https://code.visualstudio.com/)
* Install [Lens](https://k8slens.dev/)

## Deploying to Kubernetes

* Make sure your Kubernetes cluster is running and `helm` is installed.
* From the repo root run `./install.sh` (or `./charts/install.sh`) to add required repos, pull chart dependencies (including Qdrant), and install both the `databases` and `application` releases.
* Qdrant settings (persistence, resources, service ports) live under `charts/databases/values.yaml` in the `qdrant` block; adjust before installing if needed.
* To remove the releases, run `./charts/uninstall.sh`.

# OpenCIDN (Open Container Image Deliver Network)

This project is the reconfiguration of [CRProxy](https://github.com/DaoCloud/crproxy)

See more for how to deploy on [docs](https://github.com/OpenCIDN/docs)

Add the prefix `m.daocloud.io/` to all places that need to use images

## On Docker

Just add the prefix `m.daocloud.io/`

``` bash
docker pull m.daocloud.io/docker.io/library/busybox
```

## On Kubernetes

Just add the prefix `m.daocloud.io/`

``` yaml
image: m.daocloud.io/docker.io/library/busybox
```

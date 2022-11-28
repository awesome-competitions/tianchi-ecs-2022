## env
```shell
SET CGO_ENABLED=0 
SET GOOS=linux 
SET GOARCH=amd64
```
## build
```shell
go build -o kvserver
```
## ros
- 上传可执行文件到ecs
- ecs控制台，创建镜像
- 镜像共享给 1828723137315221
- 修改ros文件，修改镜像id
- 打包zip
- 上传

## performance optimize
- gnet epoll
- 减少object allocate
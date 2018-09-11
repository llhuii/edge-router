# edge-router
run this simple image to enable data forwarding to dis

``` sh
docker run -it -e DIS_STREAM_NAME=smt-poc -e DIS_TOPICS=abc --net host simple_router:latest
```

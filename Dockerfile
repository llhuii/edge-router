From golang:1.9
RUN go get github.com/eclipse/paho.mqtt.golang
RUN mkdir -p /go/src/simple_router
WORKDIR /go/src/simple_router
COPY . .
RUN go build -ldflags '-linkmode external -extldflags -static' -o simple_router simple_router.go

From scratch
COPY --from=0 /go/src/simple_router/simple_router /

ENTRYPOINT  ["/simple_router"]

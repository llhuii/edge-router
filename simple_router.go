package main

// go build -ldflags '-linkmode external -extldflags -static' -o simple_router simple_router.go

import (
	_ "bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"
	"time"

	MQTT "github.com/eclipse/paho.mqtt.golang"
)

const (
	defaultMqttUrl        = "tcp://localhost:1883"
	defaultDisUploadTopic = "SYS/dis/upload_records"
)

type (
	ServiceProperty map[string]string

	TopicServicePropertyMap map[string]ServiceProperty

	CallBackFunc func(MQTT.Client, MQTT.Message, TopicServicePropertyMap) error

	ServiceElem struct {
		Name         string
		TopicMap     TopicServicePropertyMap
		MqttSrcTopic string
		CallBack     CallBackFunc
	}

	ServiceMap map[string]*ServiceElem

	Logger interface {
		Println(v ...interface{})
		Printf(format string, v ...interface{})
	}
	NOOPLogger struct{}
)

func (NOOPLogger) Println(v ...interface{})               {}
func (NOOPLogger) Printf(format string, v ...interface{}) {}

var (
	userTopicMap map[string][]string

	serviceMap         ServiceMap
	ErrIncorrectConfig = errors.New("incorrect config")

	// DEBUG = log.New(os.Stderr, "DEBUG  ", log.Ltime)
	DEBUG Logger = NOOPLogger{}
	INFO  Logger = log.New(os.Stderr, "INFO  ", log.Ltime)
	WARN  Logger = log.New(os.Stderr, "WARN  ", log.Ltime)
	ERROR Logger = log.New(os.Stderr, "ERROR  ", log.Ltime)
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func userCallback(client MQTT.Client, msg MQTT.Message) {
	DEBUG.Printf("Topic: %s\n", msg.Topic())
	msgTopic := msg.Topic()

	for _, serviceName := range userTopicMap[msgTopic] {

		if elem, exists := serviceMap[serviceName]; exists {
			if cb := elem.CallBack; cb != nil {
				DEBUG.Printf("found callback: servicename %s for topic %s\n", serviceName, msgTopic)
				go cb(client, msg, elem.TopicMap)
			}
			continue
		}
		DEBUG.Printf("not found servicename %s for topic %s\n", serviceName, msgTopic)

	}

}

func getenv(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		value = defaultValue
	}
	return value
}

type DisRecord struct {
	Data         []byte `json:"data"`
	PartitionKey string `json:"partition_key"`
}

type DisBody struct {
	StreamName string      `json:"stream_name"`
	Records    []DisRecord `json:"records"`
}

func disValidate(sp ServiceProperty) error {
	return nil

}

func disCallback(client MQTT.Client, msg MQTT.Message, attrs TopicServicePropertyMap) error {
	payload := msg.Payload()
	topic := msg.Topic()
	streamName := attrs[topic]["stream_name"]
	partitionKey := attrs[topic]["partition_key"]
	destTopic := attrs[topic]["mqtt_topic"]
	disBody := DisBody{
		StreamName: streamName,
		Records: []DisRecord{
			{Data: payload,
				PartitionKey: partitionKey},
		},
	}
	data, err := json.Marshal(disBody)
	if err != nil {
		ERROR.Printf("encode dis for topic %s:%+v\n", topic, err)
		return err
	}
	token := client.Publish(destTopic, 0, false, data)
	if token.Wait() && token.Error() != nil {
		ERROR.Printf("uploading topic %q to dis stream %q, inter-topic %s, err %q\n", topic, streamName, destTopic, token.Error())
	}
	DEBUG.Printf("uploaded topic %q to dis stream %q, inter-topic %s\n", topic, streamName, destTopic)
	return nil
}

func registerDis() (*ServiceElem, error) {

	INFO.Printf("these environment variables are supported\n")
	INFO.Printf("\tDIS_STREAM_NAME => default stream name\n")
	INFO.Printf("\tDIS_TOPICS => mqtt source topic to be upload to dis\n")
	INFO.Printf("\t\tformat: [topic [stream_name] [partition_key];]+\n")

	service := "dis"
	disUploadTopic := getenv("DIS_UPLOAD_TOPIC", defaultDisUploadTopic)
	defaultStreamName := getenv("DIS_STREAM_NAME", "")

	// topic => service request property map
	sp := make(TopicServicePropertyMap)
	ngood := 0
	for _, f := range strings.Split(getenv("DIS_TOPICS", ""), ";") {
		fields := strings.SplitN(strings.TrimSpace(f), " ", 3)
		topic := f
		streamName := defaultStreamName
		partitionKey := "0"

		if len(fields) == 1 {
			topic = fields[0]
		} else if len(fields) == 2 {
			topic = fields[0]
			streamName = fields[1]
		} else if len(fields) == 3 {
			topic = fields[0]
			streamName = fields[1]
			partitionKey = fields[2]
		}
		topic = strings.TrimSpace(topic)

		if topic == "" {
			continue
		}

		if streamName == "" {
			WARN.Printf("stream name is empty for topic %+q, ignored\n", f)
			continue
		}

		userTopicMap[topic] = append(userTopicMap[topic], service)
		sp[topic] = ServiceProperty{
			"mqtt_topic":    disUploadTopic,
			"stream_name":   streamName,
			"partition_key": partitionKey,
		}
		ngood += 1
		INFO.Printf("Found good topic %q,%q,%q for service %s\n", topic, streamName, partitionKey, service)

	}
	if ngood == 0 {
		return nil, ErrIncorrectConfig
	}

	return &ServiceElem{
		Name:         service,
		TopicMap:     sp,
		CallBack:     disCallback,
		MqttSrcTopic: disUploadTopic,
	}, nil

}

func main() {

	mqttUrl := getenv("SYS_MQTT_URL", defaultMqttUrl)
	if getenv("LOG_LEVEL", "INFO") == "DEBUG" {
		DEBUG = log.New(os.Stderr, "DEBUG  ", log.Ltime)
	}
	userTopicMap = make(map[string][]string)
	serviceMap = make(ServiceMap)
	elem, err := registerDis()
	if err != nil {
		ERROR.Printf("can't register dis service\n")
		return
	}
	serviceMap[elem.Name] = elem

	opts := MQTT.NewClientOptions().AddBroker(mqttUrl)
	opts.SetClientID(fmt.Sprintf("simple-dis-forward-%d", rand.Intn(1000)))
	opts.SetAutoReconnect(true)

	opts.OnConnect = func(c MQTT.Client) {
		o := c.OptionsReader()
		INFO.Printf("server %+q is connected\n", o.Servers())
		for topic := range userTopicMap {

			if token := c.Subscribe(topic, 0, userCallback); token.Wait() && token.Error() != nil {
				ERROR.Printf("error on subscribe topic %s:%+q\n", topic, token.Error())
				continue
			}
			INFO.Printf("subscribe topic %s successfully", topic)
		}
	}

	c := MQTT.NewClient(opts)
	if token := c.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}
	defer c.Disconnect(250)

	for {
		time.Sleep(15 * time.Second)
	}

}

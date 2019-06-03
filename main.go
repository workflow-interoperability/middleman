package main

import (
	"encoding/json"
	"log"
	"net/url"

	"github.com/gorilla/websocket"
	"github.com/workflow-interoperability/middleman/worker"
	"github.com/workflow-interoperability/samples/worker/services"
	"github.com/zeebe-io/zeebe/clients/go/zbc"
	"gitlab.com/go-online/public-service/tools"
)

const brokerAddr = "127.0.0.1:26500"

var processID = "middleman"
var iesmid = "1"

func main() {
	client, err := zbc.NewZBClient(brokerAddr)
	if err != nil {
		panic(err)
	}

	stopChan := make(chan bool, 0)

	// define worker
	go func() {
		forwardOrderWorker := client.NewJobWorker().JobType("forwardOrder").Handler(worker.ForwardOrderWorker).Open()
		defer forwardOrderWorker.Close()
		forwardOrderWorker.AwaitClose()
	}()
	transportOrderWorker := client.NewJobWorker().JobType("transportOrder").Handler(worker.TransportOrderWorker).Open()
	defer transportOrderWorker.Close()
	transportOrderWorker.AwaitClose()

	// listen to blockchain event
	u := url.URL{Scheme: "ws", Host: "127.0.0.1:3001", Path: ""}
	c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		log.Println(err)
		return
	}
	defer c.Close()

	go func() {
		for {
			_, msg, err := c.ReadMessage()
			if err != nil {
				log.Println(err)
				return
			}
			// check message type and handle
			var structMsg map[string]interface{}
			err = json.Unmarshal(msg, &structMsg)
			if err != nil {
				log.Println(err)
				return
			}
			switch structMsg["$class"].(string) {
			case "org.sysu.wf.IMCreatedEvent":
				createSellerWorkflowInstance(structMsg["id"].(string), processID, iesmid, client)
			}
		}
	}()

	<-stopChan
}

func createSellerWorkflowInstance(imID, processID, iermID string, client zbc.ZBClient) {
	// get im
	imData, err := services.GetIM("http://127.0.0.1:3002/api/IM/" + imID)
	if err != nil {
		log.Println(err)
		return
	}
	if !(imData.Payload.WorkflowRelevantData.To.ProcessID == processID && imData.Payload.WorkflowRelevantData.To.IESMID == iermID) {
		return
	}

	// publish blockchain asset
	var data map[string]interface{}
	if imData.Payload.ApplicationData.URL != "" {
		err = json.Unmarshal([]byte(imData.Payload.ApplicationData.URL), &data)
		if err != nil {
			log.Println(err)
			return
		}
	}
	data["fromProcessInstanceID"].(map[string]string)["manufacturer"] = imData.Payload.WorkflowRelevantData.From.ProcessInstanceID
	data["processInstanceID"] = tools.GenerateXID()

	// add workflow instance
	request, err := client.NewCreateInstanceCommand().BPMNProcessId(processID).LatestVersion().VariablesFromMap(data)
	if err != nil {
		log.Println(err)
		return
	}
	msg, err := request.Send()
	if err != nil {
		log.Println(err)
		return
	}
	log.Println(msg.String())
}

package worker

import (
	"encoding/json"
	"log"
	"net/url"
	"strconv"

	"github.com/gorilla/websocket"
	"github.com/workflow-interoperability/middleman/lib"
	"github.com/workflow-interoperability/middleman/types"
	"github.com/zeebe-io/zeebe/clients/go/entities"
	"github.com/zeebe-io/zeebe/clients/go/worker"
)

// TransportOrderWorker place order
func TransportOrderWorker(client worker.JobClient, job entities.Job) {
	processID := "middleman"
	IESMID := "3"
	jobKey := job.GetKey()
	log.Println("Start transport order " + strconv.Itoa(int(jobKey)))

	payload, err := job.GetVariablesAsMap()
	if err != nil {
		log.Println(err)
		lib.FailJob(client, job)
		return
	}

	// create blockchain IM instance
	id := lib.GenerateXID()
	aData, err := json.Marshal(&payload)
	if err != nil {
		log.Println(err)
		lib.FailJob(client, job)
		return
	}
	newIM := types.IM{
		ID: id,
		Payload: types.Payload{
			ApplicationData: types.ApplicationData{
				URL: string(aData),
			},
			WorkflowRelevantData: types.WorkflowRelevantData{
				From: types.FromToData{
					ProcessID:         processID,
					ProcessInstanceID: payload["processInstanceID"].(string),
					IESMID:            IESMID,
				},
				To: types.FromToData{
					ProcessID:         "special-carrier",
					ProcessInstanceID: "-1",
					IESMID:            "1",
				},
			},
		},
		SubscriberInformation: types.SubscriberInformation{
			Roles: []string{},
			ID:    "special-carrier",
		},
	}
	pim := types.PublishIM{IM: newIM}
	body, err := json.Marshal(&pim)
	if err != nil {
		log.Println(err)
		return
	}
	err = lib.BlockchainTransaction("http://127.0.0.1:3002/api/PublishIM", string(body))
	if err != nil {
		log.Println(err)
		lib.FailJob(client, job)
		return
	}
	payload["processID"] = id
	log.Println("Publish IM success")

	// waiting for PIIS from receiver
	u := url.URL{Scheme: "ws", Host: "127.0.0.1:3003", Path: ""}
	c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		log.Println(err)
		return
	}
	defer c.Close()
	for {
		finished := false
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
			lib.FailJob(client, job)
			return
		}
		switch structMsg["$class"].(string) {
		case "org.sysu.wf.PIISCreatedEvent":
			if ok, err := publishPIIS("3003", structMsg["id"].(string), &newIM, "special-carrier", c); err != nil {
				continue
			} else if ok {
				payload["fromProcessInstanceID"].(map[string]interface{})["special-carrier"] = newIM.Payload.WorkflowRelevantData.To.ProcessInstanceID
				finished = true
				break
			}
		default:
			continue
		}
		if finished {
			log.Println("Publish PIIS success")
			break
		}
	}

	request, err := client.NewCompleteJobCommand().JobKey(jobKey).VariablesFromMap(payload)
	if err != nil {
		log.Println(err)
		lib.FailJob(client, job)
		return
	}
	request.Send()
}

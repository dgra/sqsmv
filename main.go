package main

import (
	"flag"
	"log"
	"os"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

func main() {
	src := flag.String("src", "", "source queue")
	dest := flag.String("dest", "", "destination queue")
	clients := flag.Int("clients", 1, "number of clients")
	fifo := flag.Bool("fifo", false, "is this a fifo queue?")
	flag.Parse()

	if *src == "" || *dest == "" || *clients < 1 {
		flag.Usage()
		os.Exit(1)
	}

	log.Printf("source queue : %v", *src)
	log.Printf("destination queue : %v", *dest)
	log.Printf("number of clients : %v", *clients)
	log.Printf("is this a fifo queue? : %v", *fifo)

	// enable automatic use of AWS_PROFILE like awscli and other tools do.
	opts := session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}

	sess, err := session.NewSessionWithOptions(opts)
	if err != nil {
		panic(err)
	}

	maxMessages := int64(10)
	waitTime := int64(0)
	messageAttributeNames := aws.StringSlice([]string{"All"})
	attributeNames := aws.StringSlice([]string{"All"})

	rmin := &sqs.ReceiveMessageInput{
		QueueUrl:              src,
		MaxNumberOfMessages:   &maxMessages,
		WaitTimeSeconds:       &waitTime,
		MessageAttributeNames: messageAttributeNames,
		AttributeNames:        attributeNames,
	}

	var wg sync.WaitGroup
	for i := 1; i <= *clients; i++ {
		wg.Add(i)
		go transferMessages(sess, rmin, dest, fifo, &wg)
	}
	wg.Wait()

	log.Println("all done")
}

//buildFifoMessageInput parsed out the message group id and deduplication id and
//builds a SendMessageInput suitable for a FIFO destination
func buildFifoMessageInput(m *sqs.Message, dest *string) sqs.SendMessageInput {
	messageGroupID := m.Attributes["MessageGroupId"]
	messageDeduplicationID := m.Attributes["MessageDeduplicationId"]

	log.Printf("MessageGroupId: %s", *messageGroupID)
	log.Printf("MessageDeduplicationId: %s", *messageDeduplicationID)
	// write the message to the destination queue
	return sqs.SendMessageInput{
		MessageAttributes:      m.MessageAttributes,
		MessageBody:            m.Body,
		QueueUrl:               dest,
		MessageGroupId:         messageGroupID,
		MessageDeduplicationId: messageDeduplicationID,
	}
}

//buildStandardMessageInput builds a SendMessageInput suitable for a Standard queue destination
func buildStandardMessageInput(m *sqs.Message, dest *string) sqs.SendMessageInput {
	// write the message to the destination queue
	return sqs.SendMessageInput{
		MessageAttributes: m.MessageAttributes,
		MessageBody:       m.Body,
		QueueUrl:          dest,
	}
}

//transferMessages loops, transferring a number of messages from the src to the dest at an interval.
func transferMessages(theSession *session.Session, rmin *sqs.ReceiveMessageInput, dest *string, fifo *bool, wgOuter *sync.WaitGroup) {
	client := sqs.New(theSession)

	lastMessageCount := int(1)

	defer wgOuter.Done()

	// loop as long as there are messages on the queue
	for {
		resp, err := client.ReceiveMessage(rmin)

		log.Printf("msg: %v", resp.Messages)

		if err != nil {
			panic(err)
		}

		if lastMessageCount == 0 && len(resp.Messages) == 0 {
			// no messages returned twice now, the queue is probably empty
			log.Printf("done")
			return
		}

		lastMessageCount = len(resp.Messages)
		log.Printf("received %v messages...", len(resp.Messages))

		var wg sync.WaitGroup
		wg.Add(len(resp.Messages))

		for _, m := range resp.Messages {
			go func(m *sqs.Message) {
				defer wg.Done()

				var smi sqs.SendMessageInput
				if *fifo {
					smi = buildFifoMessageInput(m, dest)
				} else {
					smi = buildStandardMessageInput(m, dest)
				}

				_, err := client.SendMessage(&smi)

				if err != nil {
					log.Printf("ERROR sending message to destination %v", err)
					return
				}

				// message was sent, dequeue from source queue
				dmi := &sqs.DeleteMessageInput{
					QueueUrl:      rmin.QueueUrl,
					ReceiptHandle: m.ReceiptHandle,
				}

				if _, err := client.DeleteMessage(dmi); err != nil {
					log.Printf("ERROR dequeueing message ID %v : %v",
						*m.ReceiptHandle,
						err)
				}
			}(m)
		}

		// wait for all jobs from this batch...
		wg.Wait()
	}
}

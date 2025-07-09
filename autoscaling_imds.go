package lifecycled

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/ec2metadata"
	"github.com/aws/aws-sdk-go/service/autoscaling"
	"github.com/sirupsen/logrus"
)

// NewAutoscalingListener ...
func NewAutoscalingImdsListener(instanceID string, autoscalingGroupName string, lifecycleHookName string, autoscaling AutoscalingClient, metadata *ec2metadata.EC2Metadata, interval time.Duration, heartbeatInterval time.Duration) *AutoscalingImdsListener {
	return &AutoscalingImdsListener{
		listenerType:         "autoscaling-imds",
		instanceID:           instanceID,
		autoscalingGroupName: autoscalingGroupName,
		lifecycleHookName:    lifecycleHookName,
		autoscaling:          autoscaling,
		metadata:             metadata,
		interval:             interval,
		heartbeatInterval:    heartbeatInterval,
	}
}

// AutoscalingListener ...
type AutoscalingImdsListener struct {
	listenerType         string
	instanceID           string
	autoscalingGroupName string
	lifecycleHookName    string
	autoscaling          AutoscalingClient
	metadata             *ec2metadata.EC2Metadata
	interval             time.Duration
	heartbeatInterval    time.Duration
}

// Type returns a string describing the listener type.
func (l *AutoscalingImdsListener) Type() string {
	return l.listenerType
}

// Start the autoscaling lifecycle hook listener.
func (l *AutoscalingImdsListener) Start(ctx context.Context, notices chan<- TerminationNotice, log *logrus.Entry) error {
	if !l.metadata.Available() {
		return errors.New("ec2 metadata is not available")
	}

	ticker := time.NewTicker(l.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			log.Debug("Polling ec2 metadata for autoscaling termination notices")

			notice := l.getTerminationNotice(log)
			if notice != nil {
				notices <- notice
				return nil
			}
		}
	}
}

func (l *AutoscalingImdsListener) getTerminationNotice(log *logrus.Entry) *autoscalingImdsTerminationNotice {
	out, err := l.metadata.GetMetadata("autoscaling/target-lifecycle-state")
	if err != nil {
		if e, ok := err.(awserr.Error); ok && strings.Contains(e.OrigErr().Error(), "404") {
			// Metadata returns 404 when there is no termination notice available
			return nil
		} else {
			log.WithError(err).Warn("Failed to get spot termination notice")
			return nil
		}
	}
	if out == "" {
		log.Error("Empty response from spot termination metadata")
		return nil
	}

	if out != "Terminated" {
		return nil
	}

	return &autoscalingImdsTerminationNotice{
		noticeType:           l.Type(),
		event:                "autoscaling:EC2_INSTANCE_TERMINATING",
		autoscaling:          l.autoscaling,
		autoscalingGroupName: l.autoscalingGroupName,
		lifecycleHookName:    l.lifecycleHookName,
		instanceID:           l.instanceID,
		heartbeatInterval:    l.heartbeatInterval,
	}
}

type autoscalingImdsTerminationNotice struct {
	noticeType           string
	event                string
	heartbeatInterval    time.Duration
	autoscaling          AutoscalingClient
	autoscalingGroupName string
	lifecycleHookName    string
	instanceID           string
}

func (n *autoscalingImdsTerminationNotice) Type() string {
	return n.noticeType
}

func (n *autoscalingImdsTerminationNotice) Handle(ctx context.Context, handler Handler, log *logrus.Entry) error {
	defer func() {
		_, err := n.autoscaling.CompleteLifecycleAction(&autoscaling.CompleteLifecycleActionInput{
			AutoScalingGroupName:  aws.String(n.autoscalingGroupName),
			LifecycleHookName:     aws.String(n.lifecycleHookName),
			InstanceId:            aws.String(n.instanceID),
			LifecycleActionResult: aws.String("CONTINUE"),
		})
		if err != nil {
			log.WithError(err).Error("Failed to complete lifecycle action")
		} else {
			log.Info("Lifecycle action completed successfully")
		}
	}()

	ticker := time.NewTicker(n.heartbeatInterval)
	defer ticker.Stop()

	go func() {
		for range ticker.C {
			log.Debug("Sending heartbeat")
			_, err := n.autoscaling.RecordLifecycleActionHeartbeat(
				&autoscaling.RecordLifecycleActionHeartbeatInput{
					AutoScalingGroupName: aws.String(n.autoscalingGroupName),
					LifecycleHookName:    aws.String(n.lifecycleHookName),
					InstanceId:           aws.String(n.instanceID),
				},
			)
			if err != nil {
				log.WithError(err).Warn("Failed to send heartbeat")
			}
		}
	}()

	return handler.Execute(ctx, n.event, n.instanceID)
}

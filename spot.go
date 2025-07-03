package lifecycled

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/ec2metadata"
	"github.com/sirupsen/logrus"
)

// NewSpotListener ...
func NewSpotListener(instanceID string, metadata *ec2metadata.EC2Metadata, interval time.Duration, rebalancesEnabled bool) *SpotListener {
	return &SpotListener{
		listenerType:      "spot",
		instanceID:        instanceID,
		metadata:          metadata,
		interval:          interval,
		rebalancesEnabled: rebalancesEnabled,
	}
}

// SpotListener ...
type SpotListener struct {
	listenerType      string
	instanceID        string
	metadata          *ec2metadata.EC2Metadata
	interval          time.Duration
	rebalancesEnabled bool
}

// Type returns a string describing the listener type.
func (l *SpotListener) Type() string {
	return l.listenerType
}

// Start the spot termination notice listener.
func (l *SpotListener) Start(ctx context.Context, notices chan<- TerminationNotice, log *logrus.Entry) error {
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
			log.Debug("Polling ec2 metadata for spot termination notices")

			notice := l.getTerminationNotice(log)
			if notice != nil {
				notices <- notice
				return nil
			}

			if l.rebalancesEnabled {
				notice = l.getRebalanceNotice(log)
				if notice != nil {
					notices <- notice
					return nil
				}
			}
		}
	}
}

func (l *SpotListener) getTerminationNotice(log *logrus.Entry) *spotNotice {
	out, err := l.metadata.GetMetadata("spot/termination-time")
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
	t, err := time.Parse(time.RFC3339, out)
	if err != nil {
		log.WithError(err).Error("Failed to parse termination time")
		return nil
	}

	return &spotNotice{
		noticeType: l.Type(),
		instanceID: l.instanceID,
		transition: "ec2:SPOT_INSTANCE_TERMINATION",
		noticeTime: t,
	}
}

func (l *SpotListener) getRebalanceNotice(log *logrus.Entry) *spotNotice {
	out, err := l.metadata.GetMetadata("events/recommendations/rebalance")
	if err != nil {
		if e, ok := err.(awserr.Error); ok && strings.Contains(e.OrigErr().Error(), "404") {
			// Metadata returns 404 when there is no termination notice available
			return nil
		} else {
			log.WithError(err).Warn("Failed to get spot rebalance notice")
			return nil
		}
	}
	if out == "" {
		log.Error("Empty response from spot rebalance metadata")
		return nil
	}

	parsed := struct {
		NoticeTime time.Time `json:"noticeTime"`
	}{}
	err = json.Unmarshal([]byte(out), &parsed)
	if err != nil {
		log.WithError(err).Error("Failed to parse rebalance notice")
		return nil
	}

	return &spotNotice{
		noticeType: l.Type(),
		instanceID: l.instanceID,
		transition: "ec2:SPOT_INSTANCE_REBALANCE",
		noticeTime: parsed.NoticeTime,
	}
}

type spotNotice struct {
	noticeType string
	instanceID string
	transition string
	noticeTime time.Time
}

func (n *spotNotice) Type() string {
	return n.noticeType
}

func (n *spotNotice) Handle(ctx context.Context, handler Handler, _ *logrus.Entry) error {
	return handler.Execute(ctx, n.transition, n.instanceID, n.noticeTime.Format(time.RFC3339))
}

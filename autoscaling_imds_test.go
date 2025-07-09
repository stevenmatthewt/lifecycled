package lifecycled

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/buildkite/lifecycled/mocks"
	"github.com/golang/mock/gomock"
	logrus "github.com/sirupsen/logrus/hooks/test"
)

type mockHandler struct {
	sleep time.Duration
	error error
}

func (h *mockHandler) Execute(ctx context.Context, args ...string) error {
	time.Sleep(h.sleep)
	return h.error
}

func TestAutoscalingImdsHandle(t *testing.T) {
	tests := []struct {
		description           string
		handler               Handler
		heartbeatInterval     time.Duration
		expectHeartbeats      bool
		expectLifecycleAction bool
		expectError           bool
	}{
		{
			description: "sends heartbeats and completes lifecycle action",
			handler: &mockHandler{
				// Long enough that we'll send at least one heartbeat
				sleep: 5 * time.Millisecond,
			},
			heartbeatInterval:     1 * time.Millisecond,
			expectHeartbeats:      true,
			expectLifecycleAction: true,
		},
		{
			description: "does not heartbeat if the handler runs faster than the heartbeat interval",
			handler: &mockHandler{
				// Handler runs much faster than the heartbeat interval
				sleep: 0 * time.Millisecond,
			},
			heartbeatInterval:     10 * time.Millisecond,
			expectHeartbeats:      false,
			expectLifecycleAction: true,
		},
		{
			description: "sends heartbeats and completes lifecycle action, even if the handler returns an error",
			handler: &mockHandler{
				// Long enough that we'll send at least one heartbeat
				sleep: 5 * time.Millisecond,
				error: fmt.Errorf("test error"),
			},
			heartbeatInterval:     1 * time.Millisecond,
			expectHeartbeats:      true,
			expectLifecycleAction: true,
			expectError:           true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.description, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockAutoscaling := mocks.NewMockAutoscalingClient(ctrl)
			if tc.expectHeartbeats {
				mockAutoscaling.EXPECT().RecordLifecycleActionHeartbeat(gomock.Any()).MinTimes(1).Return(nil, nil)
			}
			if tc.expectLifecycleAction {
				mockAutoscaling.EXPECT().CompleteLifecycleAction(gomock.Any()).Times(1).Return(nil, nil)
			}

			notice := &autoscalingImdsTerminationNotice{
				noticeType:           "autoscaling-imds",
				event:                "autoscaling:EC2_INSTANCE_TERMINATING",
				heartbeatInterval:    tc.heartbeatInterval,
				autoscaling:          mockAutoscaling,
				autoscalingGroupName: "test-group",
				lifecycleHookName:    "test-hook",
				instanceID:           "i-1234567890abcdef0",
			}

			logger, _ := logrus.NewNullLogger()
			logger.SetOutput(os.Stdout)
			ctx, cancel := context.WithTimeout(context.TODO(), 3*time.Second)
			defer cancel()

			err := notice.Handle(ctx, tc.handler, logger.WithField("notice", notice.Type()))
			if err != nil {
				if !tc.expectError {
					t.Errorf("unexpected error occured: %v", err)
				}
			} else {
				if tc.expectError {
					t.Error("expected an error to occur")
				}
			}
		})
	}
}

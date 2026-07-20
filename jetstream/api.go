package jetstream

import (
	"context"
)

type (
	apiResponse struct {
		Type  string    `json:"type"`
		Error *APIError `json:"error,omitempty"`
	}

	apiPaged struct {
		Total  int `json:"total"`
		Offset int `json:"offset"`
		Limit  int `json:"limit"`
	}
)

const (
	DefaultAPIPrefix = "$JS.API."

	jsDomainT = "$JS.%s.API."

	jsExtDomainT = "$JS.%s.API"

	apiAccountInfo = "INFO"

	apiConsumerCreateT = "CONSUMER.CREATE.%s.%s"

	apiConsumerCreateWithFilterSubjectT = "CONSUMER.CREATE.%s.%s.%s"

	apiConsumerInfoT = "CONSUMER.INFO.%s.%s"

	apiRequestNextT = "CONSUMER.MSG.NEXT.%s.%s"

	apiConsumerDeleteT = "CONSUMER.DELETE.%s.%s"

	apiConsumerPauseT = "CONSUMER.PAUSE.%s.%s"

	apiConsumerListT = "CONSUMER.LIST.%s"

	apiConsumerNamesT = "CONSUMER.NAMES.%s"

	apiStreams = "STREAM.NAMES"

	apiStreamCreateT = "STREAM.CREATE.%s"

	apiStreamInfoT = "STREAM.INFO.%s"

	apiStreamUpdateT = "STREAM.UPDATE.%s"

	apiStreamDeleteT = "STREAM.DELETE.%s"

	apiStreamPurgeT = "STREAM.PURGE.%s"

	apiStreamListT = "STREAM.LIST"

	apiMsgGetT = "STREAM.MSG.GET.%s"

	apiDirectMsgGetT = "DIRECT.GET.%s"

	apiDirectMsgGetLastBySubjectT = "DIRECT.GET.%s.%s"

	apiMsgDeleteT = "STREAM.MSG.DELETE.%s"

	apiConsumerUnpinT = "CONSUMER.UNPIN.%s.%s"

	apiConsumerResetT = "CONSUMER.RESET.%s.%s"
)

func (js *jetStream) apiRequestJSON(ctx context.Context, subject string, resp any, data ...[]byte) (*jetStreamMsg, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (js *jetStream) apiRequest(ctx context.Context, subj string, data ...[]byte) (*jetStreamMsg, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (js *jetStream) apiSubject(subj string) string { _ = "STUB: not implemented"; return "" }

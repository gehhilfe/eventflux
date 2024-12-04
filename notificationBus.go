package eventflux

import fluxcore "github.com/gehhilfe/eventflux/core"

type notificationMessageHandler struct {
	handler messageHandler
}

func newNotificationMessageHandler(
	handler messageHandler,
) *notificationMessageHandler {
	return &notificationMessageHandler{
		handler: handler,
	}
}

func (t *notificationMessageHandler) Committed(message *MessageCommittedEvent, metadata fluxcore.Metadata) error {
	return t.handler.Committed(message, metadata)
}

func (t *notificationMessageHandler) RequestResync(message *MessageRequestResync, metadata fluxcore.Metadata) error {
	return t.handler.RequestResync(message, metadata)
}

func (t *notificationMessageHandler) ResyncEvents(message *MessageResyncEvents, metadata fluxcore.Metadata) error {
	return t.handler.ResyncEvents(message, metadata)
}

func (t *notificationMessageHandler) HeartBeat(message *MessageHeartBeat, metadata fluxcore.Metadata) error {
	return t.handler.HeartBeat(message, metadata)
}

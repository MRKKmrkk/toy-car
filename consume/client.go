package consume

type consumeClient struct {
	host      string
	groupId   string
	consumeId string
}

func NewConsumeClient(host string, groupId string, consumeId string) (*consumeClient, error) {

	return &consumeClient{
		host:    host,
		groupId: groupId,
	}, nil

}

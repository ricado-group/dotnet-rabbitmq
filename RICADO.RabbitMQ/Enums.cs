namespace RICADO.RabbitMQ
{
    public enum enExchangeType
    {
        Direct,
        Fanout,
        Headers,
        Topic,
    }

    public enum enPublishMode
    {
        FireAndForget,
        BrokerConfirm,
    }

    public enum enPublishResult
    {
        Success,
        BrokerError,
        Returned,
        Timeout,
    }
}

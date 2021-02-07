namespace RICADO.RabbitMQ
{
    /// <summary>
    /// RabbitMQ Exchange Types
    /// </summary>
    public enum ExchangeType
    {
        Direct,
        Fanout,
        Headers,
        Topic,
    }

    /// <summary>
    /// Mode for Publishing Messages
    /// </summary>
    public enum PublishMode
    {
        FireAndForget,
        BrokerConfirm,
    }

    /// <summary>
    /// Results from a Message Publish Attempt
    /// </summary>
    public enum PublishResultType
    {
        Success,
        BrokerError,
        Returned,
        Timeout,
    }

    /// <summary>
    /// The Result returned by a Receiving Message Consumer
    /// </summary>
    public enum ConsumerResultType
    {
        Accept,
        Requeue,
        Discard,
    }
}

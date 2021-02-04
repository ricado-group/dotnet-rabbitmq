namespace RICADO.RabbitMQ
{
    /// <summary>
    /// RabbitMQ Exchange Types
    /// </summary>
    public enum enExchangeType
    {
        Direct,
        Fanout,
        Headers,
        Topic,
    }

    /// <summary>
    /// Mode for Publishing Messages
    /// </summary>
    public enum enPublishMode
    {
        FireAndForget,
        BrokerConfirm,
    }

    /// <summary>
    /// Results from a Message Publish Attempt
    /// </summary>
    public enum enPublishResult
    {
        Success,
        BrokerError,
        Returned,
        Timeout,
    }
}

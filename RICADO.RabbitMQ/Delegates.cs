using System;
using System.Threading;
using System.Threading.Tasks;

namespace RICADO.RabbitMQ
{
    /// <summary>
    /// A Delegate to Handle Messages received from a RabbitMQ Broker
    /// </summary>
    /// <param name="message">The Message Received</param>
    /// <param name="cancellationToken">A Cancellation Token that can be used to signal the Asynchronous Operation should be Cancelled</param>
    /// <returns>Whether the Message should be Accepted, Requeued or Discarded</returns>
    public delegate Task<ConsumerResultType> ConsumerReceiveHandler(ReceivedMessage message, CancellationToken cancellationToken);

    /// <summary>
    /// A Delegate to Handle Results from an Attempt to Publish a Message to a RabbitMQ Broker
    /// </summary>
    /// <param name="messageId">The ID of the Message</param>
    /// <param name="result">The Result of the Publish Attempt</param>
    /// <param name="failureCode">An Optional Failure Code received from the RabbitMQ Broker</param>
    /// <param name="failureReason">An Optional Failure Reason received from the RabbitMQ Broker</param>
    /// <param name="cancellationToken">A Cancellation Token that can be used to signal the Asynchronous Operation should be Cancelled</param>
    /// <returns>A Task that eventually Completes</returns>
    public delegate Task PublishResultHandler(Guid messageId, PublishResultType result, int? failureCode, string failureReason, CancellationToken cancellationToken);

    /// <summary>
    /// A Delegate to Handle Connection Exceptions raised as Events
    /// </summary>
    /// <param name="connection">The <see cref="IRabbitMQConnection"/> Instance</param>
    /// <param name="e">The Exception</param>
    public delegate void ConnectionExceptionHandler(IRabbitMQConnection connection, Exception e);

    /// <summary>
    /// A Delegate to Handle Connection Recovered Events
    /// </summary>
    /// <param name="connection">The <see cref="IRabbitMQConnection"/> Instance</param>
    public delegate void ConnectionRecoveredHandler(IRabbitMQConnection connection);

    /// <summary>
    /// A Delegate to Handle Connection Lost Events
    /// </summary>
    /// <param name="connection">The <see cref="IRabbitMQConnection"/> Instance</param>
    /// <param name="errorCode">The Code Returned by the RabbitMQ Broker</param>
    /// <param name="reason">The Reason Returned by the RabbitMQ Broker</param>
    public delegate void ConnectionLostHandler(IRabbitMQConnection connection, int errorCode, string reason);

    /// <summary>
    /// A Delegate to Handle Connection Recovery Error Events
    /// </summary>
    /// <param name="connection">The <see cref="IRabbitMQConnection"/> Instance</param>
    /// <param name="e">The Exception that occurred during Recovery</param>
    public delegate void ConnectionRecoveryErrorHandler(IRabbitMQConnection connection, Exception e);

    /// <summary>
    /// A Delegate to Handle Channel Exceptions raised as Events
    /// </summary>
    /// <param name="channel">The <see cref="IRabbitMQChannel"/> Instance</param>
    /// <param name="e">The Exception</param>
    public delegate void ChannelExceptionHandler(IRabbitMQChannel channel, Exception e);

    /// <summary>
    /// A Delegate to Handle Channel Recovered Events
    /// </summary>
    /// <param name="connection">The <see cref="IRabbitMQChannel"/> Instance</param>
    public delegate void ChannelRecoveredHandler(IRabbitMQChannel channel);

    /// <summary>
    /// A Delegate to Handle Unexpected Channel Shutdown Events
    /// </summary>
    /// <param name="channel">The <see cref="IRabbitMQChannel"/> Instance</param>
    /// <param name="errorCode">The Code Returned by the RabbitMQ Broker</param>
    /// <param name="reason">The Reason Returned by the RabbitMQ Broker</param>
    public delegate void UnexpectedChannelShutdownHandler(IRabbitMQChannel channel, int errorCode, string reason);

    /// <summary>
    /// A Delegate to Handle Channel Recovery Error Events
    /// </summary>
    /// <param name="connection">The <see cref="IRabbitMQChannel"/> Instance</param>
    /// <param name="e">The Exception that occurred during Recovery</param>
    public delegate void ChannelRecoveryErrorHandler(IRabbitMQChannel channel, Exception e);

    /// <summary>
    /// A Delegate to Handle Consumer Exceptions raised as Events
    /// </summary>
    /// <param name="channel">The <see cref="IRabbitMQConsumerChannel"/> Instance</param>
    /// <param name="e">The Exception</param>
    public delegate void ConsumerExceptionHandler(IRabbitMQConsumerChannel channel, Exception e);

    /// <summary>
    /// A Delegate to Handle Publish Exceptions raised as Events
    /// </summary>
    /// <param name="channel">The <see cref="IRabbitMQPublisherChannel"/> Instance</param>
    /// <param name="e">The Exception</param>
    public delegate void PublishExceptionHandler(IRabbitMQPublisherChannel channel, Exception e);
}

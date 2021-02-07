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
    /// A Delegate to Handle Exceptions raised as Events
    /// </summary>
    /// <param name="client">The <see cref="RabbitMQClient"/> Instance</param>
    /// <param name="e">The Exception</param>
    public delegate void ExceptionEventHandler(RabbitMQClient client, Exception e);

    /// <summary>
    /// A Delegate to Handle Connection Recovery Success Events
    /// </summary>
    /// <param name="client">The <see cref="RabbitMQClient"/> Instance</param>
    public delegate void ConnectionRecoverySuccessHandler(RabbitMQClient client);

    /// <summary>
    /// A Delegate to Handle Connection Recovery Error Events
    /// </summary>
    /// <param name="client">The <see cref="RabbitMQClient"/> Instance</param>
    /// <param name="e">The Exception that occurred during Recovery</param>
    public delegate void ConnectionRecoveryErrorHandler(RabbitMQClient client, Exception e);

    /// <summary>
    /// A Delegate to Handle Unexpected Connection Shutdown Events
    /// </summary>
    /// <param name="client">The <see cref="RabbitMQClient"/> Instance</param>
    /// <param name="errorCode">The Code Returned by the RabbitMQ Broker</param>
    /// <param name="reason">The Reason Returned by the RabbitMQ Broker</param>
    public delegate void UnexpectedConnectionShutdownHandler(RabbitMQClient client, int errorCode, string reason);
}

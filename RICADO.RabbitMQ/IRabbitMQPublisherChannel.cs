using System;
using System.Threading;
using System.Threading.Tasks;

namespace RICADO.RabbitMQ
{
    public interface IRabbitMQPublisherChannel : IRabbitMQChannel
    {
        #region Public Properties

        /// <summary>
        /// The Maximum Number of Published Messages not yet confirmed by the RabbitMQ Broker. Defaults to 10
        /// </summary>
        int UnconfirmedMessagesLimit { get; set; }

        /// <summary>
        /// The Maximum Number of Async Tasks that can be Running Concurrently to process Publish Results. Defaults to 1
        /// </summary>
        int PublishResultsConcurrency { get; set; }

        /// <summary>
        /// The Default Timeout for Message Publish Attempts
        /// </summary>
        TimeSpan DefaultPublishTimeout { get; set; }

        /// <summary>
        /// The Default Retries for Message Publish Attempts
        /// </summary>
        int DefaultPublishRetries { get; set; }

        #endregion


        #region Public Methods

        /// <summary>
        /// Register an Async Method to Handle Results for Message Publishing Attempts
        /// </summary>
        /// <param name="messageType">The Message Type this Method will Handle</param>
        /// <param name="asyncMethod">The Async Method to Handle Results</param>
        void RegisterPublishResultHandler(string messageType, PublishResultHandler asyncMethod);

        /// <summary>
        /// Unregister an Async Method to Handle Results for Message Publishing Attempts
        /// </summary>
        /// <param name="messageType">The Message Type to Unregister</param>
        void UnregisterPublishResultHandler(string messageType);

        /// <summary>
        /// Send a Message to be Published by the RabbitMQ Broker
        /// </summary>
        /// <param name="message">The Message to be Published</param>
        /// <param name="cancellationToken">A Cancellation Token that can be used to signal the Asynchronous Operation should be Cancelled</param>
        /// <returns>A Task that will Complete when the Message has been Accepted and Queued for Sending</returns>
        ValueTask Publish(PublishMessage message, CancellationToken cancellationToken);

        /// <summary>
        /// Try to Send a Message to be Published by the RabbitMQ Broker
        /// </summary>
        /// <param name="message">The Message to be Published</param>
        /// <param name="cancellationToken">A Cancellation Token that can be used to signal the Asynchronous Operation should be Cancelled</param>
        /// <returns>A Task that will Complete with a Result to indicate if the Message has been Accepted and Queued for Sending</returns>
        ValueTask<bool> TryPublish(PublishMessage message, CancellationToken cancellationToken);

        #endregion


        #region Events

        /// <summary>
        /// An Event that is Raised when an Exception occurs during a RabbitMQ Publish Attempt
        /// </summary>
        event PublishExceptionHandler PublishException;

        #endregion
    }
}

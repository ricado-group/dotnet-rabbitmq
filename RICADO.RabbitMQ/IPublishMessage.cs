using System;

namespace RICADO.RabbitMQ
{
    public interface IPublishMessage
    {
        #region Public Properties

        /// <summary>
        /// The Unique Message ID
        /// </summary>
        Guid MessageID { get; }

        /// <summary>
        /// The Exchange this Message will be Sent to
        /// </summary>
        string Exchange { get; }

        /// <summary>
        /// The Routing Key this Message will be Sent with
        /// </summary>
        string RoutingKey { get; }

        /// <summary>
        /// The Publishing Mode to handle interaction with the RabbitMQ Broker
        /// </summary>
        PublishMode Mode { get; }

        /// <summary>
        /// The Type of Message
        /// </summary>
        string Type { get; }

        /// <summary>
        /// The Message Body Byte Array
        /// </summary>
        ReadOnlyMemory<byte> Body { get; set; }

        /// <summary>
        /// Whether the Message should be Persisted by the RabbitMQ Broker
        /// </summary>
        bool Persistent { get; set; }

        /// <summary>
        /// Whether the Message should be Returned if the RabbitMQ Broker cannot Route it to a Queue
        /// </summary>
        bool Mandatory { get; set; }

        /// <summary>
        /// The Content Type for the Body
        /// </summary>
        string ContentType { get; set; }

        /// <summary>
        /// The Content Encoding for the Body
        /// </summary>
        string ContentEncoding { get; set; }

        /// <summary>
        /// An Optional Time-to-Live (TTL) for the Message
        /// </summary>
        TimeSpan? Expiration { get; set; }

        /// <summary>
        /// The Time to Wait for a Publish Response from the RabbitMQ Broker
        /// </summary>
        TimeSpan PublishTimeout { get; set; }

        /// <summary>
        /// The Number of Times to Retry Publishing
        /// </summary>
        int PublishRetries { get; set; }

        #endregion


        #region Public Methods

        /// <summary>
        /// Set the Queue Name that should be used for any Replies to this Message
        /// </summary>
        /// <param name="queueName">A Valid Queue Name</param>
        /// <returns>This Instance of <see cref="PublishMessage"/></returns>
        PublishMessage SetReplyToQueueName(string queueName);

        #endregion
    }
}

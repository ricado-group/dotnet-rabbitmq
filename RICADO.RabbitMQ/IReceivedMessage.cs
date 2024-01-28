using System;

namespace RICADO.RabbitMQ
{
    public interface IReceivedMessage
    {
        #region Public Properties

        /// <summary>
        /// The Unique Message ID
        /// </summary>
        Guid? MessageID { get; }

        /// <summary>
        /// Whether this Message has a Valid ID
        /// </summary>
        bool HasMessageID { get; }

        /// <summary>
        /// A Delivery Tag provided by the RabbitMQ Broker
        /// </summary>
        ulong DeliveryTag { get; }

        /// <summary>
        /// The Exchange this Message was originally Sent to
        /// </summary>
        string Exchange { get; }

        /// <summary>
        /// The Routing Key this Message was originally Sent with
        /// </summary>
        string RoutingKey { get; }

        /// <summary>
        /// Whether this Message has been Redelivered by the RabbitMQ Broker
        /// </summary>
        bool Redelivered { get; }

        /// <summary>
        /// The Message Body Byte Array
        /// </summary>
        ReadOnlyMemory<byte> Body { get; }

        /// <summary>
        /// Whether this Message has a Body
        /// </summary>
        bool HasBody { get; }

        /// <summary>
        /// The Type of Message
        /// </summary>
        string Type { get; }

        /// <summary>
        /// Whether this Message has a Type
        /// </summary>
        bool HasType { get; }

        /// <summary>
        /// The Content Type for the Body
        /// </summary>
        string ContentType { get; }

        /// <summary>
        /// Whether this Message has a Content Type
        /// </summary>
        bool HasContentType { get; }

        /// <summary>
        /// The Content Encoding for the Body
        /// </summary>
        string ContentEncoding { get; }

        /// <summary>
        /// Whether this Message has a Content Encoding
        /// </summary>
        bool HasContentEncoding { get; }

        /// <summary>
        /// The Name of the Application that Sent this Message
        /// </summary>
        string ApplicationName { get; }

        /// <summary>
        /// Whether this Message has an Application Name
        /// </summary>
        bool HasApplicationName { get; }

        /// <summary>
        /// Whether the Message is Persisted by the RabbitMQ Broker
        /// </summary>
        bool Persistent { get; }

        /// <summary>
        /// An Optional ID that references the Message ID being Replied To
        /// </summary>
        Guid? CorrelationID { get; }

        /// <summary>
        /// Whether this Message has a Correlation ID
        /// </summary>
        bool HasCorrelationID { get; }

        /// <summary>
        /// An Optional Queue Name to be used when requesting a Reply (RPC Style) for this Message
        /// </summary>
        string ReplyToQueueName { get; }

        /// <summary>
        /// Whether this Message has a Reply To Queue Name
        /// </summary>
        bool HasReplyToQueueName { get; }

        #endregion
    }
}

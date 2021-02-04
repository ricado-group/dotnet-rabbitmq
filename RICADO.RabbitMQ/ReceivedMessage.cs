using System;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace RICADO.RabbitMQ
{
    public class ReceivedMessage
    {
        #region Private Properties

        private Guid? _messageId;
        private ulong _deliveryTag;
        private string _exchange;
        private string _routingKey;
        private bool _redelivered;
        private ReadOnlyMemory<byte> _body;
        private string _type;
        private string _contentType;
        private string _contentEncoding;
        private string _applicationName;
        private Guid? _correlationId;
        private bool _persistent;
        private string _replyToQueueName;

        #endregion


        #region Public Properties

        /// <summary>
        /// The Unique Message ID
        /// </summary>
        public Guid? MessageID
        {
            get
            {
                return _messageId;
            }
            private set
            {
                _messageId = value;
            }
        }

        /// <summary>
        /// Whether this Message has a Valid ID
        /// </summary>
        public bool HasMessageID => _messageId.HasValue && _messageId.Value != Guid.Empty;

        /// <summary>
        /// A Delivery Tag provided by the RabbitMQ Broker
        /// </summary>
        public ulong DeliveryTag
        {
            get
            {
                return _deliveryTag;
            }
            private set
            {
                _deliveryTag = value;
            }
        }

        /// <summary>
        /// The Exchange this Message was originally Sent to
        /// </summary>
        public string Exchange
        {
            get
            {
                return _exchange;
            }
            private set
            {
                _exchange = value;
            }
        }

        /// <summary>
        /// The Routing Key this Message was originally Sent with
        /// </summary>
        public string RoutingKey
        {
            get
            {
                return _routingKey;
            }
            private set
            {
                _routingKey = value;
            }
        }

        /// <summary>
        /// Whether this Message has been Redelivered by the RabbitMQ Broker
        /// </summary>
        public bool Redelivered
        {
            get
            {
                return _redelivered;
            }
            private set
            {
                _redelivered = value;
            }
        }

        /// <summary>
        /// The Message Body Byte Array
        /// </summary>
        public ReadOnlyMemory<byte> Body
        {
            get
            {
                return _body;
            }
            private set
            {
                _body = value;
            }
        }

        /// <summary>
        /// Whether this Message has a Body
        /// </summary>
        public bool HasBody => _body.Length > 0;

        /// <summary>
        /// The Type of Message
        /// </summary>
        public string Type
        {
            get
            {
                return _type;
            }
            private set
            {
                _type = value;
            }
        }

        /// <summary>
        /// Whether this Message has a Type
        /// </summary>
        public bool HasType => (_type?.Length ?? 0) > 0;

        /// <summary>
        /// The Content Type for the Body
        /// </summary>
        public string ContentType
        {
            get
            {
                return _contentType;
            }
            private set
            {
                _contentType = value;
            }
        }

        /// <summary>
        /// Whether this Message has a Content Type
        /// </summary>
        public bool HasContentType => (_contentType?.Length ?? 0) > 0;

        /// <summary>
        /// The Content Encoding for the Body
        /// </summary>
        public string ContentEncoding
        {
            get
            {
                return _contentEncoding;
            }
            private set
            {
                _contentEncoding = value;
            }
        }

        /// <summary>
        /// Whether this Message has a Content Encoding
        /// </summary>
        public bool HasContentEncoding => (_contentEncoding?.Length ?? 0) > 0;

        /// <summary>
        /// The Name of the Application that Sent this Message
        /// </summary>
        public string ApplicationName
        {
            get
            {
                return _applicationName;
            }
            private set
            {
                _applicationName = value;
            }
        }

        /// <summary>
        /// Whether this Message has an Application Name
        /// </summary>
        public bool HasApplicationName => (_applicationName?.Length ?? 0) > 0;

        /// <summary>
        /// Whether the Message is Persisted by the RabbitMQ Broker
        /// </summary>
        public bool Persistent
        {
            get
            {
                return _persistent;
            }
            private set
            {
                _persistent = value;
            }
        }

        /// <summary>
        /// An Optional ID that references the Message ID being Replied To
        /// </summary>
        public Guid? CorrelationID
        {
            get
            {
                return _correlationId;
            }
            private set
            {
                _correlationId = value;
            }
        }

        /// <summary>
        /// Whether this Message has a Correlation ID
        /// </summary>
        public bool HasCorrelationID => _correlationId.HasValue && _correlationId.Value != Guid.Empty;

        /// <summary>
        /// An Optional Queue Name to be used when requesting a Reply (RPC Style) for this Message
        /// </summary>
        public string ReplyToQueueName
        {
            get
            {
                return _replyToQueueName;
            }
            private set
            {
                _replyToQueueName = value;
            }
        }

        /// <summary>
        /// Whether this Message has a Reply To Queue Name
        /// </summary>
        public bool HasReplyToQueueName => (_replyToQueueName?.Length ?? 0) > 0;

        #endregion


        #region Constructor

        internal ReceivedMessage()
        {
        }

        #endregion


        #region Internal Methods

        internal static ReceivedMessage CreateFromEvent(BasicDeliverEventArgs eventArgs)
        {
            if (eventArgs == null)
            {
                return null;
            }

            IBasicProperties properties = eventArgs.BasicProperties;

            ReceivedMessage message = properties?.ContentType?.ToLower() switch
            {
                ContentTypes.JSON => new JSONReceivedMessage(),
                ContentTypes.Binary => new BinaryReceivedMessage(),
                _ => new ReceivedMessage(),
            };

            message.DeliveryTag = eventArgs.DeliveryTag;
            message.Exchange = eventArgs.Exchange;
            message.RoutingKey = eventArgs.RoutingKey;
            message.Redelivered = eventArgs.Redelivered;
            message.ContentType = (properties?.IsContentTypePresent() ?? false) ? properties.ContentType : null;
            message.ContentEncoding = (properties?.IsContentEncodingPresent() ?? false) ? properties.ContentEncoding : null;

            message.ExpandBody(eventArgs.Body);

            if (properties == null)
            {
                return message;
            }

            message.Type = properties.IsTypePresent() ? properties.Type : null;
            message.Persistent = properties.IsDeliveryModePresent() ? properties.Persistent : false;
            message.ApplicationName = properties.IsAppIdPresent() ? properties.AppId : null;
            message.ReplyToQueueName = properties.IsReplyToPresent() ? properties.ReplyTo : null;

            Guid messageId;

            if(properties.IsMessageIdPresent() && Guid.TryParse(properties.MessageId, out messageId))
            {
                message.MessageID = messageId;
            }

            Guid correlationId;

            if(properties.IsCorrelationIdPresent() && Guid.TryParse(properties.CorrelationId, out correlationId))
            {
                message.CorrelationID = correlationId;
            }

            return message;
        }

        #endregion


        #region Protected Methods

        /// <summary>
        /// Expand the Received Body Bytes
        /// </summary>
        /// <param name="bytes">The Received Bytes</param>
        protected virtual void ExpandBody(ReadOnlyMemory<byte> bytes)
        {
            byte[] bodyBytes = new byte[bytes.Length];

            bytes.CopyTo(bodyBytes);

            _body = bodyBytes;
        }

        #endregion
    }
}

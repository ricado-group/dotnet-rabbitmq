using System;
using System.Collections.Concurrent;
using System.Threading;
using RabbitMQ.Client;

namespace RICADO.RabbitMQ
{
    public class PublishMessage : IPublishMessage
    {
        #region Private Fields

        private readonly Guid _messageId;
        private string _exchange;
        private string _routingKey;
        private PublishMode _mode;
        private string _type;

        private ReadOnlyMemory<byte> _body;
        private bool _persistent;
        private bool _mandatory;
        private string _contentType;
        private string _contentEncoding;
        private TimeSpan? _expiration;
        private TimeSpan _publishTimeout;
        private int _publishRetries;

        private Guid? _correlationId;
        private string _replyTo;

        private ulong _deliveryTag = 0;
        private readonly object _deliveryTagLock = new object();

        private DateTime _publishTimestamp = DateTime.UtcNow;
        private readonly object _publishTimestampLock = new object();

        private readonly CountdownEvent _retriesCountdown;

        #endregion


        #region Internal Properties

        /// <summary>
        /// An Optional ID that references the Message ID being Replied To
        /// </summary>
        internal Guid? CorrelationID
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
        /// An Optional Queue Name to be used when requesting a Reply (RPC Style) for this Message
        /// </summary>
        internal string ReplyTo
        {
            get
            {
                return _replyTo;
            }
            private set
            {
                _replyTo = value;
            }
        }

        /// <summary>
        /// A Delivery Tag that will be provided to the RabbitMQ Broker
        /// </summary>
        internal ulong DeliveryTag
        {
            get
            {
                lock (_deliveryTagLock)
                {
                    return _deliveryTag;
                }
            }
        }

        /// <summary>
        /// The Timestamp of the last Publish Attempt
        /// </summary>
        internal DateTime PublishTimestamp
        {
            get
            {
                lock (_publishTimestampLock)
                {
                    return _publishTimestamp;
                }
            }
        }

        /// <summary>
        /// Whether this Message can be Retried for Publishing
        /// </summary>
        internal bool HasRetriesAvailable
        {
            get
            {
                return _retriesCountdown.IsSet == false;
            }
        }

        #endregion


        #region Public Properties

        public Guid MessageID
        {
            get
            {
                return _messageId;
            }
        }

        public string Exchange
        {
            get
            {
                return _exchange;
            }
            protected set
            {
                _exchange = value;
            }
        }

        public string RoutingKey
        {
            get
            {
                return _routingKey;
            }
            protected set
            {
                _routingKey = value;
            }
        }

        public PublishMode Mode
        {
            get
            {
                return _mode;
            }
            protected set
            {
                _mode = value;
            }
        }

        public string Type
        {
            get
            {
                return _type;
            }
            protected set
            {
                _type = value;
            }
        }

        public ReadOnlyMemory<byte> Body
        {
            get
            {
                return _body;
            }
            set
            {
                _body = value;
            }
        }

        public bool Persistent
        {
            get
            {
                return _persistent;
            }
            set
            {
                _persistent = value;
            }
        }

        public bool Mandatory
        {
            get
            {
                return _mandatory;
            }
            set
            {
                _mandatory = value;
            }
        }

        public string ContentType
        {
            get
            {
                return _contentType;
            }
            set
            {
                _contentType = value;
            }
        }

        public string ContentEncoding
        {
            get
            {
                return _contentEncoding;
            }
            set
            {
                _contentEncoding = value;
            }
        }

        public TimeSpan? Expiration
        {
            get
            {
                return _expiration;
            }
            set
            {
                _expiration = value;
            }
        }

        public TimeSpan PublishTimeout
        {
            get
            {
                return _publishTimeout;
            }
            set
            {
                _publishTimeout = value;
            }
        }

        public int PublishRetries
        {
            get
            {
                return _publishRetries;
            }
            set
            {
                _publishRetries = value;

                _retriesCountdown.Reset(value);
            }
        }

        #endregion


        #region Constructor

        /// <summary>
        /// Create a new <see cref="PublishMessage"/> Instance
        /// </summary>
        /// <param name="initialPublishRetries">The Initial Retry Count for this Message</param>
        protected PublishMessage(int initialPublishRetries)
        {
            _messageId = Guid.NewGuid();

            _retriesCountdown = new CountdownEvent(initialPublishRetries);
        }

        #endregion


        #region Public Methods

        /// <summary>
        /// Create a new Message for Publishing to an Exchange
        /// </summary>
        /// <param name="channel">A <see cref="IRabbitMQPublisherChannel"/> Instance</param>
        /// <param name="exchange">The Exchange to Publish this Message to</param>
        /// <param name="routingKey">The Routing Key for this Message</param>
        /// <param name="type">The Type of Message</param>
        /// <param name="mode">The Publishing Mode for this Message</param>
        /// <returns>A New <see cref="PublishMessage"/> Instance ready to be Customized before Publishing</returns>
        public static PublishMessage CreateNew(IRabbitMQPublisherChannel channel, string exchange, string routingKey, string type = "", PublishMode mode = PublishMode.BrokerConfirm)
        {
            if(channel == null)
            {
                throw new ArgumentNullException(nameof(channel));
            }
            
            if(exchange == null)
            {
                throw new ArgumentNullException(nameof(exchange));
            }

            if(routingKey == null)
            {
                throw new ArgumentNullException(nameof(routingKey));
            }

            return new PublishMessage(channel.DefaultPublishRetries)
            {
                Exchange = exchange,
                RoutingKey = routingKey,
                Mode = mode,
                Type = type,
                PublishTimeout = channel.DefaultPublishTimeout,
                PublishRetries = channel.DefaultPublishRetries,
            };
        }

        /// <summary>
        /// Create a new Message for Publishing a Reply directly to a Queue
        /// </summary>
        /// <param name="channel">A <see cref="IRabbitMQPublisherChannel"/> Instance</param>
        /// <param name="replyTo">The Name of the Queue to Directly Publish to</param>
        /// <param name="receivedMessageId">The ID of the Message that is being Replied to</param>
        /// <param name="type">The Type of Message</param>
        /// <param name="mode">The Publishing Mode for this Message</param>
        /// <returns>A New <see cref="PublishMessage"/> Instance ready to be Customized before Publishing</returns>
        public static PublishMessage CreateNew(IRabbitMQPublisherChannel channel, string replyTo, Guid receivedMessageId, string type = "", PublishMode mode = PublishMode.BrokerConfirm)
        {
            if (channel == null)
            {
                throw new ArgumentNullException(nameof(channel));
            }

            if (replyTo == null)
            {
                throw new ArgumentNullException(nameof(replyTo));
            }

            if (receivedMessageId == Guid.Empty)
            {
                throw new ArgumentOutOfRangeException(nameof(receivedMessageId), "The Received Message ID cannot be an Empty GUID");
            }

            return new PublishMessage(channel.DefaultPublishRetries)
            {
                Exchange = "",
                RoutingKey = replyTo,
                Mode = mode,
                Type = type,
                PublishTimeout = channel.DefaultPublishTimeout,
                PublishRetries = channel.DefaultPublishRetries,
                CorrelationID = receivedMessageId,
            };
        }

        public PublishMessage SetReplyToQueueName(string queueName)
        {
            ReplyTo = queueName;

            return this;
        }

        #endregion


        #region Internal Methods

        internal IBasicProperties BuildProperties(IModel channel)
        {
            IBasicProperties properties = channel.CreateBasicProperties();

            properties.MessageId = _messageId.ToString();

            properties.Type = _type;

            properties.Persistent = _persistent;

            if(_contentType != null)
            {
                properties.ContentType = _contentType;
            }

            if(_contentEncoding != null)
            {
                properties.ContentEncoding = _contentEncoding;
            }

            if(_expiration.HasValue && _expiration.Value.TotalMilliseconds >= 0)
            {
                properties.Expiration = Convert.ToInt32(Math.Floor(_expiration.Value.TotalMilliseconds)).ToString();
            }

            if(_correlationId.HasValue && _correlationId.Value != Guid.Empty)
            {
                properties.CorrelationId = _correlationId.Value.ToString();
            }

            if(_replyTo != null && _replyTo.Length > 0)
            {
                properties.ReplyTo = _replyTo;
            }

            return properties;
        }

        internal void Publish(IModel channel, ConcurrentDictionary<Guid, PublishMessage> publishMessages)
        {
            lock(_deliveryTagLock)
            {
                _deliveryTag = channel.NextPublishSeqNo;
            }

            lock(_publishTimestampLock)
            {
                _publishTimestamp = DateTime.UtcNow;
            }

            publishMessages.TryAdd(_messageId, this);

            channel.BasicPublish(Exchange, RoutingKey, Mandatory, BuildProperties(channel), Body);

            lock (_publishTimestampLock)
            {
                _publishTimestamp = DateTime.UtcNow;
            }

            _retriesCountdown.Signal();
        }

        internal void UpdateFailedPublish()
        {
            lock (_publishTimestampLock)
            {
                _publishTimestamp = DateTime.UtcNow;
            }

            _retriesCountdown.Signal();
        }

        #endregion
    }
}

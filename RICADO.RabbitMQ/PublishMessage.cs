using System;
using System.Threading;
using RabbitMQ.Client;

namespace RICADO.RabbitMQ
{
    public class PublishMessage
    {
        #region Private Properties

        private Guid _messageId;
        private string _exchange;
        private string _routingKey;
        private enPublishMode _mode;
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
        private object _deliveryTagLock = new object();

        private DateTime _publishTimestamp = DateTime.MinValue;
        private object _publishTimestampLock = new object();

        private CountdownEvent _retriesCountdown;

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

        public enPublishMode Mode
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

        internal DateTime PublishTimestamp
        {
            get
            {
                lock(_publishTimestampLock)
                {
                    return _publishTimestamp;
                }
            }
        }

        internal bool HasRetriesAvailable
        {
            get
            {
                return _retriesCountdown.IsSet;
            }
        }

        #endregion


        #region Constructor

        private PublishMessage(int initialPublishRetries)
        {
            _messageId = Guid.NewGuid();

            _retriesCountdown = new CountdownEvent(initialPublishRetries);
        }

        #endregion


        #region Public Methods

        public static PublishMessage CreateNew(RabbitMQClient client, string exchange, string routingKey, string type = "", enPublishMode mode = enPublishMode.BrokerConfirm)
        {
            if(client == null)
            {
                throw new ArgumentNullException(nameof(client));
            }
            
            if(exchange == null)
            {
                throw new ArgumentNullException(nameof(exchange));
            }

            if(routingKey == null)
            {
                throw new ArgumentNullException(nameof(routingKey));
            }

            return new PublishMessage(client.DefaultPublishRetries)
            {
                Exchange = exchange,
                RoutingKey = routingKey,
                Mode = mode,
                Type = type,
                PublishTimeout = client.DefaultPublishTimeout.HasValue ? client.DefaultPublishTimeout.Value : client.HeartbeatInterval.Multiply(2),
                PublishRetries = client.DefaultPublishRetries,
            };
        }

        public static PublishMessage CreateNew(RabbitMQClient client, string replyTo, Guid receivedMessageId, string type = "", enPublishMode mode = enPublishMode.BrokerConfirm)
        {
            if (client == null)
            {
                throw new ArgumentNullException(nameof(client));
            }

            if (replyTo == null)
            {
                throw new ArgumentNullException(nameof(replyTo));
            }

            if (receivedMessageId == Guid.Empty)
            {
                throw new ArgumentOutOfRangeException(nameof(receivedMessageId), "The Received Message ID cannot be an Empty GUID");
            }

            return new PublishMessage(client.DefaultPublishRetries)
            {
                Exchange = "",
                RoutingKey = replyTo,
                Mode = mode,
                Type = type,
                PublishTimeout = client.DefaultPublishTimeout.HasValue ? client.DefaultPublishTimeout.Value : client.HeartbeatInterval.Multiply(2),
                PublishRetries = client.DefaultPublishRetries,
                CorrelationID = receivedMessageId,
            };
        }

        public PublishMessage SetReplyToQueueName(string queueName)
        {
            this.ReplyTo = queueName;

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

        internal void Publish(IModel channel)
        {
            lock(_deliveryTagLock)
            {
                _deliveryTag = channel.NextPublishSeqNo;
            }

            channel.BasicPublish(Exchange, RoutingKey, Mandatory, BuildProperties(channel), Body);

            lock (_publishTimestampLock)
            {
                _publishTimestamp = DateTime.Now;
            }

            _retriesCountdown.Signal();
        }

        internal void UpdateFailedPublish()
        {
            lock (_publishTimestampLock)
            {
                _publishTimestamp = DateTime.Now;
            }

            _retriesCountdown.Signal();
        }

        #endregion


        #region Private Methods

        

        #endregion
    }
}

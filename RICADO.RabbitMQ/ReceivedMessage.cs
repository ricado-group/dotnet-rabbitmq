using System;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace RICADO.RabbitMQ
{
    public class ReceivedMessage : IReceivedMessage
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

        public bool HasMessageID => _messageId.HasValue && _messageId.Value != Guid.Empty;

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

        public bool HasBody => _body.Length > 0;

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

        public bool HasType => (_type?.Length ?? 0) > 0;

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

        public bool HasContentType => (_contentType?.Length ?? 0) > 0;

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

        public bool HasContentEncoding => (_contentEncoding?.Length ?? 0) > 0;

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

        public bool HasApplicationName => (_applicationName?.Length ?? 0) > 0;

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

        public bool HasCorrelationID => _correlationId.HasValue && _correlationId.Value != Guid.Empty;

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

            ReceivedMessage message;

            switch(properties?.ContentType?.ToLower())
            {
                case ContentTypes.JSON:
                    message = new JSONReceivedMessage();
                    break;

                case ContentTypes.Binary:
                    message = new BinaryReceivedMessage();
                    break;

                default:
                    message = new ReceivedMessage();
                    break;
            }

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
            message.Persistent = properties.IsDeliveryModePresent() && properties.Persistent;
            message.ApplicationName = properties.IsAppIdPresent() ? properties.AppId : null;
            message.ReplyToQueueName = properties.IsReplyToPresent() ? properties.ReplyTo : null;

            if(properties.IsMessageIdPresent() && Guid.TryParse(properties.MessageId, out Guid messageId))
            {
                message.MessageID = messageId;
            }

            if(properties.IsCorrelationIdPresent() && Guid.TryParse(properties.CorrelationId, out Guid correlationId))
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

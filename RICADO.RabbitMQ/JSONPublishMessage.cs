﻿using System;
using System.Text;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace RICADO.RabbitMQ
{
    public class JSONPublishMessage : PublishMessage
    {
        #region Constructor

        /// <summary>
        /// Create a new <see cref="JSONPublishMessage"/> Instance
        /// </summary>
        /// <param name="initialPublishRetries">The Initial Retry Count for this Message</param>
        protected JSONPublishMessage(int initialPublishRetries) : base(initialPublishRetries)
        {
        }

        #endregion


        #region Public Methods

        /// <summary>
        /// Create a new Message with JSON Content for Publishing to an Exchange
        /// </summary>
        /// <param name="client">A <see cref="RabbitMQClient"/> Instance</param>
        /// <param name="exchange">The Exchange to Publish this Message to</param>
        /// <param name="routingKey">The Routing Key for this Message</param>
        /// <param name="json">The JSON Data for this Message</param>
        /// <param name="type">The Type of Message</param>
        /// <param name="mode">The Publishing Mode for this Message</param>
        /// <returns>A New <see cref="JSONPublishMessage"/> Instance ready to be Published</returns>
        public static PublishMessage CreateNew(RabbitMQClient client, string exchange, string routingKey, JToken json, string type = "", enPublishMode mode = enPublishMode.BrokerConfirm)
        {
            if(json == null)
            {
                throw new ArgumentNullException(nameof(json));
            }

            PublishMessage message = PublishMessage.CreateNew(client, exchange, routingKey, type, mode);

            message.Body = Encoding.UTF8.GetBytes(json.ToString(Formatting.None));
            message.ContentType = ContentTypes.JSON;
            message.ContentEncoding = "utf8";

            return message;
        }

        /// <summary>
        /// Create a new Message with JSON Content for Publishing a Reply directly to a Queue
        /// </summary>
        /// <param name="client">A <see cref="RabbitMQClient"/> Instance</param>
        /// <param name="replyTo">The Name of the Queue to Directly Publish to</param>
        /// <param name="receivedMessageId">The ID of the Message that is being Replied to</param>
        /// <param name="json">The JSON Data for this Message</param>
        /// <param name="type">The Type of Message</param>
        /// <param name="mode">The Publishing Mode for this Message</param>
        /// <returns>A New <see cref="JSONPublishMessage"/> Instance ready to be Published</returns>
        public static PublishMessage CreateNew(RabbitMQClient client, string replyTo, Guid receivedMessageId, JToken json, string type = "", enPublishMode mode = enPublishMode.BrokerConfirm)
        {
            if (json == null)
            {
                throw new ArgumentNullException(nameof(json));
            }

            PublishMessage message = PublishMessage.CreateNew(client, replyTo, receivedMessageId, type, mode);

            message.Body = Encoding.UTF8.GetBytes(json.ToString(Formatting.None));
            message.ContentType = ContentTypes.JSON;
            message.ContentEncoding = "utf8";

            return message;
        }

        #endregion
    }
}

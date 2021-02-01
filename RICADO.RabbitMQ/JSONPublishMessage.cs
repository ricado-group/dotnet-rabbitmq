using System;
using System.Text;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace RICADO.RabbitMQ
{
    public static class JSONPublishMessage
    {
        #region Public Methods

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

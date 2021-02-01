using System;

namespace RICADO.RabbitMQ
{
    public static class BinaryPublishMessage
    {
        #region Public Methods

        public static PublishMessage CreateNew(RabbitMQClient client, string exchange, string routingKey, ReadOnlyMemory<byte> bytes, string type = "", enPublishMode mode = enPublishMode.BrokerConfirm)
        {
            PublishMessage message = PublishMessage.CreateNew(client, exchange, routingKey, type, mode);

            message.Body = bytes;
            message.ContentType = ContentTypes.Binary;

            return message;
        }

        public static PublishMessage CreateNew(RabbitMQClient client, string replyTo, Guid receivedMessageId, ReadOnlyMemory<byte> bytes, string type = "", enPublishMode mode = enPublishMode.BrokerConfirm)
        {
            PublishMessage message = PublishMessage.CreateNew(client, replyTo, receivedMessageId, type, mode);

            message.Body = bytes;
            message.ContentType = ContentTypes.Binary;

            return message;
        }

        #endregion
    }
}
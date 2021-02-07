using System;

namespace RICADO.RabbitMQ
{
    public class BinaryPublishMessage : PublishMessage
    {
        #region Constructor

        /// <summary>
        /// Create a new <see cref="BinaryPublishMessage"/> Instance
        /// </summary>
        /// <param name="initialPublishRetries">The Initial Retry Count for this Message</param>
        protected BinaryPublishMessage(int initialPublishRetries) : base(initialPublishRetries)
        {
        }

        #endregion


        #region Public Methods

        /// <summary>
        /// Create a new Message with Binary (byte) Content for Publishing to an Exchange
        /// </summary>
        /// <param name="client">A <see cref="RabbitMQClient"/> Instance</param>
        /// <param name="exchange">The Exchange to Publish this Message to</param>
        /// <param name="routingKey">The Routing Key for this Message</param>
        /// <param name="bytes">The Binary (byte) Data for this Message</param>
        /// <param name="type">The Type of Message</param>
        /// <param name="mode">The Publishing Mode for this Message</param>
        /// <returns>A New <see cref="BinaryPublishMessage"/> Instance ready to be Published</returns>
        public static PublishMessage CreateNew(RabbitMQClient client, string exchange, string routingKey, ReadOnlyMemory<byte> bytes, string type = "", PublishMode mode = PublishMode.BrokerConfirm)
        {
            PublishMessage message = CreateNew(client, exchange, routingKey, type, mode);

            message.Body = bytes;
            message.ContentType = ContentTypes.Binary;

            return message;
        }

        /// <summary>
        /// Create a new Message with Binary (byte) Content for Publishing a Reply directly to a Queue
        /// </summary>
        /// <param name="client">A <see cref="RabbitMQClient"/> Instance</param>
        /// <param name="replyTo">The Name of the Queue to Directly Publish to</param>
        /// <param name="receivedMessageId">The ID of the Message that is being Replied to</param>
        /// <param name="bytes">The Binary (byte) Data for this Message</param>
        /// <param name="type">The Type of Message</param>
        /// <param name="mode">The Publishing Mode for this Message</param>
        /// <returns>A New <see cref="BinaryPublishMessage"/> Instance ready to be Published</returns>
        public static PublishMessage CreateNew(RabbitMQClient client, string replyTo, Guid receivedMessageId, ReadOnlyMemory<byte> bytes, string type = "", PublishMode mode = PublishMode.BrokerConfirm)
        {
            PublishMessage message = CreateNew(client, replyTo, receivedMessageId, type, mode);

            message.Body = bytes;
            message.ContentType = ContentTypes.Binary;

            return message;
        }

        #endregion
    }
}
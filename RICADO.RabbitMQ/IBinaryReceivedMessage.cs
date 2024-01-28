using System;

namespace RICADO.RabbitMQ
{
    public interface IBinaryReceivedMessage : IReceivedMessage
    {
        #region Public Properties

        /// <summary>
        /// The Binary Message Body
        /// </summary>
        new ReadOnlyMemory<byte> Body { get; }

        #endregion
    }
}

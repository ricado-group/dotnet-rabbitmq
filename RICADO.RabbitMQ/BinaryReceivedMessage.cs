using System;

namespace RICADO.RabbitMQ
{
    public class BinaryReceivedMessage : ReceivedMessage
    {
        #region Private Properties

        

        #endregion


        #region Public Properties

        

        #endregion


        #region Constructor

        internal BinaryReceivedMessage()
        {
        }

        #endregion


        #region Public Methods


        #endregion


        #region Protected Methods

        protected override void ExpandBody(ReadOnlyMemory<byte> bytes)
        {
            base.ExpandBody(bytes);
        }

        #endregion
    }
}

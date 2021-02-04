using System;

namespace RICADO.RabbitMQ
{
    public class BinaryReceivedMessage : ReceivedMessage
    {
        #region Private Properties

        private ReadOnlyMemory<byte> _binaryBody;

        #endregion


        #region Public Properties

        /// <summary>
        /// The JSON Message Body
        /// </summary>
        public new ReadOnlyMemory<byte> Body
        {
            get
            {
                return _binaryBody;
            }
        }

        #endregion


        #region Constructor

        internal BinaryReceivedMessage()
        {
        }

        #endregion


        #region Protected Methods

        /// <summary>
        /// Expand the Received Body Bytes into a Binary (byte) Array
        /// </summary>
        /// <param name="bytes">The Received Bytes</param>
        protected override void ExpandBody(ReadOnlyMemory<byte> bytes)
        {
            base.ExpandBody(bytes);

            _binaryBody = base.Body.ToArray();
        }

        #endregion
    }
}

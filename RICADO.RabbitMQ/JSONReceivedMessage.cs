using System;
using System.Text;
using Newtonsoft.Json.Linq;

namespace RICADO.RabbitMQ
{
    public class JSONReceivedMessage : ReceivedMessage
    {
        #region Private Properties

        private JToken _jsonBody;

        #endregion


        #region Public Properties

        /// <summary>
        /// The JSON Message Body
        /// </summary>
        public new JToken Body
        {
            get
            {
                return _jsonBody;
            }
        }

        #endregion


        #region Constructor

        internal JSONReceivedMessage()
        {
        }

        #endregion


        #region Protected Methods

        /// <summary>
        /// Expand the Received Body Bytes into a JSON Token (JToken)
        /// </summary>
        /// <param name="bytes">The Received Bytes</param>
        protected override void ExpandBody(ReadOnlyMemory<byte> bytes)
        {
            base.ExpandBody(bytes);

            if(base.Body.Length == 0)
            {
                _jsonBody = JValue.CreateNull();
                return;
            }

            string jsonString = Encoding.UTF8.GetString(base.Body.ToArray());

            if(jsonString == null || jsonString.Length == 0)
            {
                _jsonBody = JValue.CreateNull();
            }

            try
            {
                _jsonBody = JToken.Parse(jsonString);
            }
            catch
            {
                _jsonBody = JValue.CreateNull();
            }
        }

        #endregion
    }
}

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


        #region Public Methods


        #endregion


        #region Protected Methods

        protected override void ExpandBody(ReadOnlyMemory<byte> bytes)
        {
            base.ExpandBody(bytes);

            if(base.Body.Length == 0)
            {
                _jsonBody = JToken.FromObject(null); // TODO: Confirm this actually works!
                return;
            }

            string jsonString = UTF8Encoding.UTF8.GetString(base.Body.ToArray());

            if(jsonString == null || jsonString.Length == 0)
            {
                _jsonBody = JToken.FromObject(null); // TODO: Confirm if this will work or whether we should call JToken.Parse on the Null / Empty String?
            }

            _jsonBody = JToken.Parse(jsonString);
        }

        #endregion
    }
}

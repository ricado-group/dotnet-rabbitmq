using Newtonsoft.Json.Linq;

namespace RICADO.RabbitMQ
{
    public interface IJSONReceivedMessage : IReceivedMessage
    {
        #region Public Properties

        /// <summary>
        /// The JSON Message Body
        /// </summary>
        new JToken Body { get; }

        #endregion
    }
}

using System;

namespace RICADO.RabbitMQ
{
    public class RabbitMQException : Exception
    {
        #region Constructors

        internal RabbitMQException(string message) : base(message)
        {
        }

        internal RabbitMQException(string message, Exception innerException) : base(message, innerException)
        {
        }

        #endregion
    }
}

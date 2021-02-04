using System;

namespace RICADO.RabbitMQ
{
    /// <summary>
    /// Represents Errors that have occurred in the RabbitMQ Library
    /// </summary>
    public class RabbitMQException : Exception
    {
        #region Constructors

        /// <summary>
        /// Initialize a new instance of the <see cref="RabbitMQException"/> class with the specified Message
        /// </summary>
        /// <param name="message">The Message that describes this Error</param>
        internal RabbitMQException(string message) : base(message)
        {
        }

        /// <summary>
        /// Initialize a new instance of the <see cref="RabbitMQException"/> class with the specified Message and Inner Exception
        /// </summary>
        /// <param name="message">The Message that describes this Error</param>
        /// <param name="innerException">The Inner Exception that caused or contributed to this Error</param>
        internal RabbitMQException(string message, Exception innerException) : base(message, innerException)
        {
        }

        #endregion
    }
}

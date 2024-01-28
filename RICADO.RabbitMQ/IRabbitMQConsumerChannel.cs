using System.Threading;
using System.Threading.Tasks;

namespace RICADO.RabbitMQ
{
    public interface IRabbitMQConsumerChannel : IRabbitMQChannel
    {
        #region Public Properties

        /// <summary>
        /// The Maximum Number of Unacknowledged Messages that can be Delivered by the RabbitMQ Broker. Defaults to 1
        /// </summary>
        ushort PrefetchCount { get; set; }

        #endregion


        #region Public Methods

        /// <summary>
        /// Create a Consumer that will call the provided Async Method when a Message is Received from the specified Queue
        /// </summary>
        /// <param name="queueName">The Name of the Queue to Start Consuming</param>
        /// <param name="asyncMethod">The Async Method that will Handle Received Messages</param>
        /// <param name="cancellationToken">A Cancellation Token that can be used to signal the Asynchronous Operation should be Cancelled</param>
        /// <returns>A Task that will Complete upon successfully Creating a Queue Consumer</returns>
        Task CreateQueueConsumer(string queueName, ConsumerReceiveHandler asyncMethod, CancellationToken cancellationToken);

        /// <summary>
        /// Destroy a Queue Consumer
        /// </summary>
        /// <param name="queueName">The Name of the Queue to Stop Consuming</param>
        /// <param name="cancellationToken">A Cancellation Token that can be used to signal the Asynchronous Operation should be Cancelled</param>
        /// <returns>A Task that will Complete upon successfully Destroying a Queue Consumer</returns>
        Task DestroyQueueConsumer(string queueName, CancellationToken cancellationToken);

        #endregion


        #region Events

        /// <summary>
        /// An Event that is Raised when an Exception occurs in a RabbitMQ Consumer
        /// </summary>
        event ConsumerExceptionHandler ConsumerException;

        #endregion
    }
}

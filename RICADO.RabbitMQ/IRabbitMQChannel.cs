using System.Threading;
using System.Threading.Tasks;

namespace RICADO.RabbitMQ
{
    public interface IRabbitMQChannel
    {
        #region Public Properties
        
        /// <summary>
        /// Whether this Channel is Connected to the RabbitMQ Broker
        /// </summary>
        bool IsConnected { get; }

        /// <summary>
        /// Whether the RabbitMQ Channel is Shutdown and no longer able to be used
        /// </summary>
        bool IsShutdown { get; }

        #endregion


        #region Public Methods

        /// <summary>
        /// Initialize this <see cref="IRabbitMQChannel"/> Instance
        /// </summary>
        /// <param name="cancellationToken">A Cancellation Token that can be used to signal the Asynchronous Operation should be Cancelled</param>
        /// <returns>A Task that will Complete upon successful Initialization</returns>
        Task Initialize(CancellationToken cancellationToken);

        /// <summary>
        /// Destroy this <see cref="IRabbitMQChannel"/> Instance
        /// </summary>
        /// <param name="cancellationToken">A Cancellation Token that can be used to signal the Asynchronous Operation should be Cancelled</param>
        /// <returns>A Task that will Complete when this Instance is Destroyed</returns>
        Task Destroy(CancellationToken cancellationToken);

        /// <summary>
        /// Declare an Exchange
        /// </summary>
        /// <param name="name">The Name of the Exchange to Declare</param>
        /// <param name="type">The Type of Exchange to Declare</param>
        /// <param name="durable">Whether the Exchange should survive a RabbitMQ Broker Restart</param>
        /// <param name="autoDelete">Whether the Exchange should be Deleted Automatically when it is no longer Bound or this Client Disconnects</param>
        /// <param name="cancellationToken">A Cancellation Token that can be used to signal the Asynchronous Operation should be Cancelled</param>
        /// <returns>A Task that will Complete upon successfully Declaring an Exchange</returns>
        Task DeclareExchange(string name, ExchangeType type, bool durable, bool autoDelete, CancellationToken cancellationToken);

        /// <summary>
        /// Confirm an Exchange with the specified Name exists
        /// </summary>
        /// <param name="name">The Name of the Exchange to Passively Declare</param>
        /// <param name="cancellationToken">A Cancellation Token that can be used to signal the Asynchronous Operation should be Cancelled</param>
        /// <returns>A Task that will Complete upon successfully Confirming the Exchange exists</returns>
        Task DeclareExchangePassive(string name, CancellationToken cancellationToken);

        /// <summary>
        /// Delete an Exchange
        /// </summary>
        /// <param name="name">The Name of the Exchange to Delete</param>
        /// <param name="ifUnused">Whether the Exchange should only be Deleted if it is Unused</param>
        /// <param name="cancellationToken">A Cancellation Token that can be used to signal the Asynchronous Operation should be Cancelled</param>
        /// <returns>A Task that will Complete upon successfully Deleting an Exchange</returns>
        Task DeleteExchange(string name, bool ifUnused, CancellationToken cancellationToken);

        /// <summary>
        /// Declare a Queue
        /// </summary>
        /// <param name="name">The Name of the Queue to Declare</param>
        /// <param name="durable">Whether the Queue should survive a RabbitMQ Broker Restart</param>
        /// <param name="exclusive">Whether the Queue can be used by other Clients and Processes</param>
        /// <param name="autoDelete">Whether the Queue should be Deleted Automatically when it is no longer Bound or this Client Disconnects</param>
        /// <param name="cancellationToken">A Cancellation Token that can be used to signal the Asynchronous Operation should be Cancelled</param>
        /// <param name="deadLetterExchangeName">An Optional Name of the Exchange to Send Messages that are Dead-Lettered</param>
        /// <returns>A Task that will Complete upon successfully Declaring a Queue</returns>
        Task DeclareQueue(string name, bool durable, bool exclusive, bool autoDelete, CancellationToken cancellationToken, string deadLetterExchangeName = null);

        /// <summary>
        /// Confirm a Queue with the specified Name exists
        /// </summary>
        /// <param name="name">The Name of the Queue to Passively Declare</param>
        /// <param name="cancellationToken">A Cancellation Token that can be used to signal the Asynchronous Operation should be Cancelled</param>
        /// <returns>A Task that will Complete upon successfully Confirming the Queue exists</returns>
        Task DeclareQueuePassive(string name, CancellationToken cancellationToken);

        /// <summary>
        /// Delete a Queue
        /// </summary>
        /// <param name="name">The Name of the Queue to Delete</param>
        /// <param name="ifUnused">Whether the Queue should only be Deleted if it is Unused</param>
        /// <param name="ifEmpty">Whether the Queue should only be Deleted if it is Empty</param>
        /// <param name="cancellationToken">A Cancellation Token that can be used to signal the Asynchronous Operation should be Cancelled</param>
        /// <returns>A Task that will Complete upon successfully Deleting a Queue</returns>
        Task<uint> DeleteQueue(string name, bool ifUnused, bool ifEmpty, CancellationToken cancellationToken);

        /// <summary>
        /// Bind an Exchange to another Exchange
        /// </summary>
        /// <param name="sourceName">The Name of the Source Exchange</param>
        /// <param name="destinationName">The Name of the Destination Exchange</param>
        /// <param name="routingKey">The Routing Key for this Exchange Binding</param>
        /// <param name="cancellationToken">A Cancellation Token that can be used to signal the Asynchronous Operation should be Cancelled</param>
        /// <returns>A Task that will Complete upon successfully Binding an Exchange to another Exchange</returns>
        Task BindExchangeToExchange(string sourceName, string destinationName, string routingKey, CancellationToken cancellationToken);

        /// <summary>
        /// Unbind an Exchange from another Exchange
        /// </summary>
        /// <param name="sourceName">The Name of the Source Exchange</param>
        /// <param name="destinationName">The Name of the Destination Exchange</param>
        /// <param name="routingKey">The Routing Key for this Exchange Binding</param>
        /// <param name="cancellationToken">A Cancellation Token that can be used to signal the Asynchronous Operation should be Cancelled</param>
        /// <returns>A Task that will Complete upon successfully Unbinding an Exchange from another Exchange</returns>
        Task UnbindExchangeFromExchange(string sourceName, string destinationName, string routingKey, CancellationToken cancellationToken);

        /// <summary>
        /// Bind a Queue to an Exchange
        /// </summary>
        /// <param name="queueName">The Name of the Queue to be Bound</param>
        /// <param name="exchangeName">The Name of the Exchange to Bind to</param>
        /// <param name="routingKey">The Routing Key for this Queue Binding</param>
        /// <param name="cancellationToken">A Cancellation Token that can be used to signal the Asynchronous Operation should be Cancelled</param>
        /// <returns>A Task that will Complete upon successfully Binding a Queue to an Exchange</returns>
        Task BindQueueToExchange(string queueName, string exchangeName, string routingKey, CancellationToken cancellationToken);

        /// <summary>
        /// Unbind a Queue from an Exchange
        /// </summary>
        /// <param name="queueName">The Name of the Queue to Unbind</param>
        /// <param name="exchangeName">The Name of the Exchange</param>
        /// <param name="routingKey">The Routing Key for this Queue Binding</param>
        /// <param name="cancellationToken">A Cancellation Token that can be used to signal the Asynchronous Operation should be Cancelled</param>
        /// <returns>A Task that will Complete upon successfully Unbinding a Queue from an Exchange</returns>
        Task UnbindQueueFromExchange(string queueName, string exchangeName, string routingKey, CancellationToken cancellationToken);

        #endregion


        #region Events

        /// <summary>
        /// An Event that is Raised when an Exception occurs on this RabbitMQ Channel
        /// </summary>
        event ChannelExceptionHandler ChannelException;

        /// <summary>
        /// An Event that is Raised when the RabbitMQ Channel is unexpectedly Shutdown and is no longer useable
        /// </summary>
        event UnexpectedChannelShutdownHandler UnexpectedChannelShutdown;

        #endregion
    }
}

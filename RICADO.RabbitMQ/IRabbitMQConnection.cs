using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace RICADO.RabbitMQ
{
    public interface IRabbitMQConnection
    {
        #region Public Properties

        /// <summary>
        /// The Name for this Connection (shown in the RabbitMQ Management UI)
        /// </summary>
        string ConnectionName { get; }

        /// <summary>
        /// The Name of the Application that is utilizing this Connection
        /// </summary>
        string ApplicationName { get; }

        /// <summary>
        /// The Username for Authentication with the RabbitMQ Broker
        /// </summary>
        string Username { get; }

        /// <summary>
        /// The Password for Authentication with RabbitMQ Broker
        /// </summary>
        string Password { get; }

        /// <summary>
        /// The VirtualHost to utilize on the RabbitMQ Broker
        /// </summary>
        string VirtualHost { get; }

        /// <summary>
        /// A Collection of DNS Names or IP Addresses for RabbitMQ Brokers
        /// </summary>
        ICollection<string> Servers { get; }

        /// <summary>
        /// The Port of the RabbitMQ Broker - Defaults to <code>5672</code> for Non-TLS Connections and <code>5671</code> for TLS Connections
        /// </summary>
        int Port { get; set; }

        /// <summary>
        /// The Maximum Number of Async Tasks that can be Running Concurrently to process Received Messages
        /// </summary>
        int ConsumerConcurrency { get; set; }

        /// <summary>
        /// The Duration allowed before a Connection Attempt will Timeout
        /// </summary>
        TimeSpan ConnectionTimeout { get; set; }

        /// <summary>
        /// The Interval between Connection Recovery Attempts
        /// </summary>
        TimeSpan ConnectionRecoveryInterval { get; set; }

        /// <summary>
        /// The Delay between sending Heartbeat Messages to the RabbitMQ Broker
        /// </summary>
        TimeSpan HeartbeatInterval { get; set; }

        /// <summary>
        /// The Maximum Duration before a Socket Read will Timeout
        /// </summary>
        TimeSpan ReadTimeout { get; set; }

        /// <summary>
        /// The Maximum Duration before a Socket Write will Timeout
        /// </summary>
        TimeSpan WriteTimeout { get; set; }

        /// <summary>
        /// Whether this Connection is Connected to the RabbitMQ Broker
        /// </summary>
        bool IsConnected { get; }

        /// <summary>
        /// Whether the RabbitMQ Connection is Shutdown and no longer able to be used
        /// </summary>
        bool IsShutdown { get; }

        #endregion


        #region Public Methods

        /// <summary>
        /// Initialize this <see cref="IRabbitMQConnection"/> Instance
        /// </summary>
        /// <param name="cancellationToken">A Cancellation Token that can be used to signal the Asynchronous Operation should be Cancelled</param>
        /// <returns>A Task that will Complete upon successful Initialization</returns>
        Task Initialize(CancellationToken cancellationToken);

        /// <summary>
        /// Destroy this <see cref="IRabbitMQConnection"/> Instance
        /// </summary>
        /// <param name="cancellationToken">A Cancellation Token that can be used to signal the Asynchronous Operation should be Cancelled</param>
        /// <returns>A Task that will Complete when this Instance is Destroyed</returns>
        Task Destroy(CancellationToken cancellationToken);

        /// <summary>
        /// Create a new RabbitMQ Channel used for Consuming Messages
        /// </summary>
        /// <returns>A new Instance of a RabbitMQ Consumer Channel</returns>
        IRabbitMQConsumerChannel CreateConsumerChannel();

        /// <summary>
        /// Create a new RabbitMQ Channel used for Publishing Messages
        /// </summary>
        /// <returns>A new Instance of a RabbitMQ Publisher Channel</returns>
        IRabbitMQPublisherChannel CreatePublisherChannel();

        #endregion


        #region Events

        /// <summary>
        /// An Event that is Raised when an Exception occurs on this RabbitMQ Connection
        /// </summary>
        event ConnectionExceptionHandler ConnectionException;

        /// <summary>
        /// An Event that is Raised when the RabbitMQ Connection has Recovered
        /// </summary>
        event ConnectionRecoveredHandler ConnectionRecovered;

        /// <summary>
        /// An Event that is Raised when the RabbitMQ Connection has been Lost
        /// </summary>
        event ConnectionLostHandler ConnectionLost;

        /// <summary>
        /// An Event that is Raised when the RabbitMQ Connection Fails to Recover with an Exception
        /// </summary>
        event ConnectionRecoveryErrorHandler ConnectionRecoveryError;

        #endregion
    }
}

using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Channels;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;
using RabbitMQ.Client.Events;

namespace RICADO.RabbitMQ
{
    public class RabbitMQClient
    {
        #region Constants

        internal const int DefaultConnectionTimeout = 2000;
        internal const int DefaultConnectionRecoveryInterval = 2000;
        internal const int DefaultHeartbeatInterval = 5000;
        internal const int DefaultReadTimeout = 4000;
        internal const int DefaultWriteTimeout = 2000;

        #endregion


        #region Private Properties

        private readonly string _clientName;
        private readonly string _applicationName;
        private readonly string _username;
        private readonly string _password;
        private readonly string _virtualHost;
        private readonly ICollection<string> _servers;

        private ConnectionFactory _connectionFactory;
        private IAutorecoveringConnection _connection;
        private IModel _channel;
        private AsyncEventingBasicConsumer _consumer;
        private CancellationTokenSource _consumerCts;

        private readonly ConcurrentDictionary<string, ConsumerReceiveHandler> _consumerAsyncMethods = new ConcurrentDictionary<string, ConsumerReceiveHandler>();
        private readonly ConcurrentDictionary<Guid, RedeliveredMessage> _consumerRedeliveredMessages = new ConcurrentDictionary<Guid, RedeliveredMessage>();

        private readonly SemaphoreSlim _channelSemaphore = new SemaphoreSlim(1, 1);

        private readonly ConcurrentDictionary<Guid, PublishMessage> _publishMessages = new ConcurrentDictionary<Guid, PublishMessage>();
        private readonly SemaphoreSlim _publishSemaphore = new SemaphoreSlim(1, 1);
        private Task _publishMessagesTask;
        private CancellationTokenSource _publishMessagesCts;

        private readonly ConcurrentDictionary<string, PublishResultHandler> _publishResultsAsyncMethods = new ConcurrentDictionary<string, PublishResultHandler>();
        private Channel<PublishResult> _publishResultsChannel;
        private SemaphoreSlim _publishResultsSemaphore;
        private CancellationTokenSource _publishResultsCts;
        private Task _publishResultsTask;

        private TimeSpan? _defaultPublishTimeout;
        private int _defaultPublishRetries = 3;

        private bool _connectionShutdown = false;
        private readonly object _connectionShutdownLock = new object();

        #endregion


        #region Public Properties

        /// <summary>
        /// The Name for this Client (shown in the RabbitMQ Management UI)
        /// </summary>
        public string ClientName => _clientName;

        /// <summary>
        /// The Name of the Application that is utilizing this Client
        /// </summary>
        public string ApplicationName => _applicationName;

        /// <summary>
        /// The Username for Authentication with the RabbitMQ Broker
        /// </summary>
        public string Username => _username;

        /// <summary>
        /// The Password for Authentication with RabbitMQ Broker
        /// </summary>
        public string Password => _password;

        /// <summary>
        /// The VirtualHost to utilize on the RabbitMQ Broker
        /// </summary>
        public string VirtualHost => _virtualHost;

        /// <summary>
        /// A Collection of DNS Names or IP Addresses for RabbitMQ Brokers
        /// </summary>
        public ICollection<string> Servers => _servers;

        /// <summary>
        /// The Port of the RabbitMQ Broker - Defaults to <code>5672</code> for Non-TLS Connections and <code>5671</code> for TLS Connections
        /// </summary>
        public int Port
        {
            get
            {
                return _connectionFactory.Port;
            }
            set
            {
                _connectionFactory.Port = value;
            }
        }

        /// <summary>
        /// The Maximum Number of Async Tasks that can be Running Concurrently to process Received Messages
        /// </summary>
        public int ConsumerConcurrency
        {
            get
            {
                return _connectionFactory.ConsumerDispatchConcurrency;
            }
            set
            {
                _connectionFactory.ConsumerDispatchConcurrency = value;
            }
        }

        /// <summary>
        /// The Duration allowed before a Connection Attempt will Timeout
        /// </summary>
        public TimeSpan ConnectionTimeout
        {
            get
            {
                return _connectionFactory.RequestedConnectionTimeout;
            }
            set
            {
                _connectionFactory.RequestedConnectionTimeout = value;
            }
        }

        /// <summary>
        /// The Interval between Connection Recovery Attempts
        /// </summary>
        public TimeSpan ConnectionRecoveryInterval
        {
            get
            {
                return _connectionFactory.NetworkRecoveryInterval;
            }
            set
            {
                _connectionFactory.NetworkRecoveryInterval = value;
            }
        }

        /// <summary>
        /// The Delay between sending Heartbeat Messages to the RabbitMQ Broker
        /// </summary>
        public TimeSpan HeartbeatInterval
        {
            get
            {
                return _connectionFactory.RequestedHeartbeat;
            }
            set
            {
                _connectionFactory.RequestedHeartbeat = value;
            }
        }

        /// <summary>
        /// The Maximum Duration before a Socket Read will Timeout
        /// </summary>
        public TimeSpan ReadTimeout
        {
            get
            {
                return _connectionFactory.SocketReadTimeout;
            }
            set
            {
                _connectionFactory.SocketReadTimeout = value;

                _connectionFactory.ContinuationTimeout = _connectionFactory.SocketReadTimeout + _connectionFactory.SocketWriteTimeout;
                _connectionFactory.HandshakeContinuationTimeout = _connectionFactory.SocketReadTimeout + _connectionFactory.SocketWriteTimeout;
            }
        }

        /// <summary>
        /// The Maximum Duration before a Socket Write will Timeout
        /// </summary>
        public TimeSpan WriteTimeout
        {
            get
            {
                return _connectionFactory.SocketWriteTimeout;
            }
            set
            {
                _connectionFactory.SocketWriteTimeout = value;

                _connectionFactory.ContinuationTimeout = _connectionFactory.SocketReadTimeout + _connectionFactory.SocketWriteTimeout;
                _connectionFactory.HandshakeContinuationTimeout = _connectionFactory.SocketReadTimeout + _connectionFactory.SocketWriteTimeout;
            }
        }

        /// <summary>
        /// The Default Timeout for Message Publish Attempts
        /// </summary>
        public TimeSpan? DefaultPublishTimeout
        {
            get
            {
                return _defaultPublishTimeout;
            }
            set
            {
                _defaultPublishTimeout = value;
            }
        }

        /// <summary>
        /// The Default Retries for Message Publish Attempts
        /// </summary>
        public int DefaultPublishRetries
        {
            get
            {
                return _defaultPublishRetries;
            }
            set
            {
                _defaultPublishRetries = value;
            }
        }

        /// <summary>
        /// Whether this Client is Connected to the RabbitMQ Broker
        /// </summary>
        public bool IsConnected => _channel?.IsOpen ?? false;

        /// <summary>
        /// Whether the Client Connection is Shutdown and no longer able to be used
        /// </summary>
        public bool IsShutdown
        {
            get
            {
                lock(_connectionShutdownLock)
                {
                    return _connectionShutdown;
                }
            }
            private set
            {
                lock(_connectionShutdownLock)
                {
                    _connectionShutdown = value;
                }
            }
        }

        #endregion


        #region Constructor

        /// <summary>
        /// Create a new <see cref="RabbitMQClient"/> Instance
        /// </summary>
        /// <param name="clientName">The Name for this Client (shown in the RabbitMQ Management UI)</param>
        /// <param name="applicationName">The Name of the Application that is utilizing this Client</param>
        /// <param name="username">The Username for Authentication with the RabbitMQ Broker</param>
        /// <param name="password">The Password for Authentication with RabbitMQ Broker</param>
        /// <param name="virtualHost">The VirtualHost to utilize on the RabbitMQ Broker</param>
        /// <param name="servers">A Collection of DNS Names or IP Addresses for RabbitMQ Brokers</param>
        /// <param name="useSsl">Whether to use a TLS or Non-TLS Connection</param>
        /// <param name="sslCommonName">The Common Name (CN) of the SSL Certificate used by the RabbitMQ Broker</param>
        public RabbitMQClient(string clientName, string applicationName, string username, string password, string virtualHost, ICollection<string> servers, bool useSsl = false, string sslCommonName = null)
        {
            if (clientName == null)
            {
                throw new ArgumentNullException(nameof(clientName));
            }

            if (clientName.Length == 0)
            {
                throw new ArgumentOutOfRangeException(nameof(clientName), "The Client Name cannot be Empty");
            }

            _clientName = clientName;

            if (applicationName == null)
            {
                throw new ArgumentNullException(nameof(applicationName));
            }

            if (applicationName.Length == 0)
            {
                throw new ArgumentOutOfRangeException(nameof(applicationName), "The Application Name cannot be Empty");
            }

            _applicationName = applicationName;

            if (username == null)
            {
                throw new ArgumentNullException(nameof(username));
            }

            if (username.Length == 0)
            {
                throw new ArgumentOutOfRangeException(nameof(username), "The Username cannot be Empty");
            }

            _username = username;

            if (password == null)
            {
                throw new ArgumentNullException(nameof(password));
            }

            if (password.Length == 0)
            {
                throw new ArgumentOutOfRangeException(nameof(password), "The Password cannot be Empty");
            }

            _password = password;

            if (virtualHost == null)
            {
                throw new ArgumentNullException(nameof(virtualHost));
            }

            if (virtualHost.Length == 0)
            {
                throw new ArgumentOutOfRangeException(nameof(virtualHost), "The Virtual Host cannot be Empty");
            }

            _virtualHost = virtualHost;

            if (servers == null)
            {
                throw new ArgumentNullException(nameof(servers));
            }

            if (servers.Count == 0)
            {
                throw new ArgumentOutOfRangeException(nameof(servers), "The Servers Collection cannot be Empty");
            }

            _servers = servers;

            if (useSsl == true && sslCommonName == null)
            {
                throw new ArgumentNullException(nameof(sslCommonName), "The SSL Common Name cannot be Null when SSL Mode is selected");
            }

            if (useSsl == true && sslCommonName.Length == 0)
            {
                throw new ArgumentOutOfRangeException(nameof(sslCommonName), "The SSL Common Name cannot be Empty when SSL Mode is selected");
            }

            try
            {
                _connectionFactory = new ConnectionFactory()
                {
                    ClientProvidedName = _clientName,
                    UserName = _username,
                    Password = _password,
                    VirtualHost = _virtualHost,

                    AutomaticRecoveryEnabled = true,
                    TopologyRecoveryEnabled = true,
                    UseBackgroundThreadsForIO = true,
                    DispatchConsumersAsync = true,
                    ConsumerDispatchConcurrency = 1,

                    Port = AmqpTcpEndpoint.UseDefaultPort,

                    ContinuationTimeout = TimeSpan.FromMilliseconds(DefaultReadTimeout + DefaultWriteTimeout),
                    HandshakeContinuationTimeout = TimeSpan.FromMilliseconds(DefaultReadTimeout + DefaultWriteTimeout),
                    NetworkRecoveryInterval = TimeSpan.FromMilliseconds(DefaultConnectionRecoveryInterval),
                    RequestedConnectionTimeout = TimeSpan.FromMilliseconds(DefaultConnectionTimeout),
                    RequestedHeartbeat = TimeSpan.FromMilliseconds(DefaultHeartbeatInterval),
                    SocketReadTimeout = TimeSpan.FromMilliseconds(DefaultReadTimeout),
                    SocketWriteTimeout = TimeSpan.FromMilliseconds(DefaultWriteTimeout),
                };

                if(useSsl == true)
                {
                    _connectionFactory.Port = AmqpTcpEndpoint.DefaultAmqpSslPort;
                    _connectionFactory.AmqpUriSslProtocols = System.Security.Authentication.SslProtocols.Tls12;
                    _connectionFactory.Ssl = new SslOption()
                    {
                        Enabled = true,
                        ServerName = sslCommonName,
                        Version = System.Security.Authentication.SslProtocols.Tls12,
                    };
                }
            }
            catch (Exception e)
            {
                throw new RabbitMQException("Unexpected Connection Factory Exception", e);
            }
        }

        #endregion


        #region Public Methods

        /// <summary>
        /// Initialize this <see cref="RabbitMQClient"/> Instance
        /// </summary>
        /// <param name="cancellationToken">A Cancellation Token that can be used to signal the Asynchronous Operation should be Cancelled</param>
        /// <returns>A Task that will Complete upon successful Initialization</returns>
        public async Task Initialize(CancellationToken cancellationToken)
        {
            IsShutdown = false;
            
            await initializeConnection(cancellationToken);

            await initializeChannel(cancellationToken);

            _publishResultsChannel = Channel.CreateUnbounded<PublishResult>(new UnboundedChannelOptions { SingleReader = true, SingleWriter = false, AllowSynchronousContinuations = false });
            _publishResultsSemaphore = new SemaphoreSlim(ConsumerConcurrency, ConsumerConcurrency);
            _publishResultsCts = new CancellationTokenSource();
            _publishResultsTask = Task.Run(publishResultsHandler, CancellationToken.None);

            _publishMessagesCts = new CancellationTokenSource();
            _publishMessagesTask = Task.Run(publishMessagesHandler, CancellationToken.None);
        }

        /// <summary>
        /// Destroy this <see cref="RabbitMQClient"/> Instance
        /// </summary>
        /// <param name="cancellationToken">A Cancellation Token that can be used to signal the Asynchronous Operation should be Cancelled</param>
        /// <returns>A Task that will Complete when this Instance is Destroyed</returns>
        public async Task Destroy(CancellationToken cancellationToken)
        {
            _publishResultsChannel?.Writer?.Complete();

            _publishResultsCts?.Cancel();

            _publishMessagesCts?.Cancel();

            IsShutdown = true;

            await _channelSemaphore.WaitAsync(cancellationToken);

            try
            {
                destroyChannel();
            }
            finally
            {
                _channelSemaphore.Release();
            }

            destroyConnection();

            _connectionFactory = null;

            _consumerAsyncMethods.Clear();

            _consumerRedeliveredMessages.Clear();

            _publishMessages.Clear();

            try
            {
                if (_publishResultsTask != null)
                {
                    await _publishResultsTask;
                }
            }
            catch
            {
            }

            _publishResultsAsyncMethods.Clear();
            _publishResultsChannel = null;
            _publishResultsTask = null;

            try
            {
                if (_publishMessagesTask != null)
                {
                    await _publishMessagesTask;
                }
            }
            catch
            {
            }

            _publishMessagesTask = null;
        }

        /// <summary>
        /// Declare an Exchange
        /// </summary>
        /// <param name="name">The Name of the Exchange to Declare</param>
        /// <param name="type">The Type of Exchange to Declare</param>
        /// <param name="durable">Whether the Exchange should survive a RabbitMQ Broker Restart</param>
        /// <param name="autoDelete">Whether the Exchange should be Deleted Automatically when it is no longer Bound or this Client Disconnects</param>
        /// <param name="cancellationToken">A Cancellation Token that can be used to signal the Asynchronous Operation should be Cancelled</param>
        /// <returns>A Task that will Complete upon successfully Declaring an Exchange</returns>
        public async Task DeclareExchange(string name, ExchangeType type, bool durable, bool autoDelete, CancellationToken cancellationToken)
        {
            if (name == null)
            {
                throw new ArgumentNullException(nameof(name));
            }

            if (IsShutdown == true)
            {
                throw new RabbitMQException("Cannot Declare an Exchange since the Connection is Shutdown");
            }

            await _channelSemaphore.WaitAsync(cancellationToken);

            try
            {
                if (IsConnected == false)
                {
                    throw new RabbitMQException("Cannot Declare an Exchange while the Connection is Unavailable");
                }

                _channel.ExchangeDeclare(name, type.ToString().ToLower(), durable, autoDelete);
            }
            finally
            {
                _channelSemaphore.Release();
            }
        }

        /// <summary>
        /// Confirm an Exchange with the specified Name exists
        /// </summary>
        /// <param name="name">The Name of the Exchange to Passively Declare</param>
        /// <param name="cancellationToken">A Cancellation Token that can be used to signal the Asynchronous Operation should be Cancelled</param>
        /// <returns>A Task that will Complete upon successfully Confirming the Exchange exists</returns>
        public async Task DeclareExchangePassive(string name, CancellationToken cancellationToken)
        {
            if (name == null)
            {
                throw new ArgumentNullException(nameof(name));
            }

            if (IsShutdown == true)
            {
                throw new RabbitMQException("Cannot Passively Declare an Exchange since the Connection is Shutdown");
            }

            await _channelSemaphore.WaitAsync(cancellationToken);

            try
            {
                if (IsConnected == false)
                {
                    throw new RabbitMQException("Cannot Passively Declare an Exchange while the Connection is Unavailable");
                }

                _channel.ExchangeDeclarePassive(name);
            }
            finally
            {
                _channelSemaphore.Release();
            }
        }

        /// <summary>
        /// Delete an Exchange
        /// </summary>
        /// <param name="name">The Name of the Exchange to Delete</param>
        /// <param name="ifUnused">Whether the Exchange should only be Deleted if it is Unused</param>
        /// <param name="cancellationToken">A Cancellation Token that can be used to signal the Asynchronous Operation should be Cancelled</param>
        /// <returns>A Task that will Complete upon successfully Deleting an Exchange</returns>
        public async Task DeleteExchange(string name, bool ifUnused, CancellationToken cancellationToken)
        {
            if (name == null)
            {
                throw new ArgumentNullException(nameof(name));
            }

            if (IsShutdown == true)
            {
                throw new RabbitMQException("Cannot Delete an Exchange since the Connection is Shutdown");
            }

            await _channelSemaphore.WaitAsync(cancellationToken);

            try
            {
                if (IsConnected == false)
                {
                    throw new RabbitMQException("Cannot Delete an Exchange while the Connection is Unavailable");
                }

                _channel.ExchangeDelete(name, ifUnused);
            }
            finally
            {
                _channelSemaphore.Release();
            }
        }

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
        public async Task DeclareQueue(string name, bool durable, bool exclusive, bool autoDelete, CancellationToken cancellationToken, string deadLetterExchangeName = null)
        {
            if (name == null)
            {
                throw new ArgumentNullException(nameof(name));
            }

            if (IsShutdown == true)
            {
                throw new RabbitMQException("Cannot Declare a Queue since the Connection is Shutdown");
            }

            await _channelSemaphore.WaitAsync(cancellationToken);

            try
            {
                if (IsConnected == false)
                {
                    throw new RabbitMQException("Cannot Declare a Queue while the Connection is Unavailable");
                }

                Dictionary<string, object> properties = new Dictionary<string, object>();

                if (deadLetterExchangeName != null && deadLetterExchangeName.Length > 0)
                {
                    properties.Add("x-dead-letter-exchange", deadLetterExchangeName);
                }

                QueueDeclareOk result = _channel.QueueDeclare(name, durable, exclusive, autoDelete, properties);

                if (result == null || result.QueueName != name)
                {
                    throw new RabbitMQException("Failed to Declare the Queue - The Broker Queue Name did not match the Client Queue Name");
                }
            }
            finally
            {
                _channelSemaphore.Release();
            }
        }

        /// <summary>
        /// Confirm a Queue with the specified Name exists
        /// </summary>
        /// <param name="name">The Name of the Queue to Passively Declare</param>
        /// <param name="cancellationToken">A Cancellation Token that can be used to signal the Asynchronous Operation should be Cancelled</param>
        /// <returns>A Task that will Complete upon successfully Confirming the Queue exists</returns>
        public async Task DeclareQueuePassive(string name, CancellationToken cancellationToken)
        {
            if (name == null)
            {
                throw new ArgumentNullException(nameof(name));
            }

            if (IsShutdown == true)
            {
                throw new RabbitMQException("Cannot Passively Declare a Queue since the Connection is Shutdown");
            }

            await _channelSemaphore.WaitAsync(cancellationToken);

            try
            {
                if (IsConnected == false)
                {
                    throw new RabbitMQException("Cannot Passively Declare a Queue while the Connection is Unavailable");
                }

                QueueDeclareOk result = _channel.QueueDeclarePassive(name);

                if (result == null || (name.Length > 0 && result.QueueName != name))
                {
                    throw new RabbitMQException("Failed to Passively Declare the Queue - The Broker Queue Name did not match the Client Queue Name");
                }
            }
            finally
            {
                _channelSemaphore.Release();
            }
        }

        /// <summary>
        /// Delete a Queue
        /// </summary>
        /// <param name="name">The Name of the Queue to Delete</param>
        /// <param name="ifUnused">Whether the Queue should only be Deleted if it is Unused</param>
        /// <param name="ifEmpty">Whether the Queue should only be Deleted if it is Empty</param>
        /// <param name="cancellationToken">A Cancellation Token that can be used to signal the Asynchronous Operation should be Cancelled</param>
        /// <returns>A Task that will Complete upon successfully Deleting a Queue</returns>
        public async Task<uint> DeleteQueue(string name, bool ifUnused, bool ifEmpty, CancellationToken cancellationToken)
        {
            if (name == null)
            {
                throw new ArgumentNullException(nameof(name));
            }

            if (IsShutdown == true)
            {
                throw new RabbitMQException("Cannot Delete a Queue since the Connection is Shutdown");
            }

            await _channelSemaphore.WaitAsync(cancellationToken);

            try
            {
                if (IsConnected == false)
                {
                    throw new RabbitMQException("Cannot Delete a Queue while the Connection is Unavailable");
                }

                return _channel.QueueDelete(name, ifUnused, ifEmpty);
            }
            finally
            {
                _channelSemaphore.Release();
            }
        }

        /// <summary>
        /// Bind an Exchange to another Exchange
        /// </summary>
        /// <param name="sourceName">The Name of the Source Exchange</param>
        /// <param name="destinationName">The Name of the Destination Exchange</param>
        /// <param name="routingKey">The Routing Key for this Exchange Binding</param>
        /// <param name="cancellationToken">A Cancellation Token that can be used to signal the Asynchronous Operation should be Cancelled</param>
        /// <returns>A Task that will Complete upon successfully Binding an Exchange to another Exchange</returns>
        public async Task BindExchangeToExchange(string sourceName, string destinationName, string routingKey, CancellationToken cancellationToken)
        {
            if (sourceName == null)
            {
                throw new ArgumentNullException(nameof(sourceName));
            }

            if (destinationName == null)
            {
                throw new ArgumentNullException(nameof(destinationName));
            }

            if (routingKey == null)
            {
                throw new ArgumentNullException(nameof(routingKey));
            }

            if (IsShutdown == true)
            {
                throw new RabbitMQException("Cannot Bind an Exchange to another Exchange since the Connection is Shutdown");
            }

            await _channelSemaphore.WaitAsync(cancellationToken);

            try
            {
                if (IsConnected == false)
                {
                    throw new RabbitMQException("Cannot Bind an Exchange to another Exchange while the Connection is Unavailable");
                }

                _channel.ExchangeBind(destinationName, sourceName, routingKey);
            }
            finally
            {
                _channelSemaphore.Release();
            }
        }

        /// <summary>
        /// Unbind an Exchange from another Exchange
        /// </summary>
        /// <param name="sourceName">The Name of the Source Exchange</param>
        /// <param name="destinationName">The Name of the Destination Exchange</param>
        /// <param name="routingKey">The Routing Key for this Exchange Binding</param>
        /// <param name="cancellationToken">A Cancellation Token that can be used to signal the Asynchronous Operation should be Cancelled</param>
        /// <returns>A Task that will Complete upon successfully Unbinding an Exchange from another Exchange</returns>
        public async Task UnbindExchangeFromExchange(string sourceName, string destinationName, string routingKey, CancellationToken cancellationToken)
        {
            if (sourceName == null)
            {
                throw new ArgumentNullException(nameof(sourceName));
            }

            if (destinationName == null)
            {
                throw new ArgumentNullException(nameof(destinationName));
            }

            if (routingKey == null)
            {
                throw new ArgumentNullException(nameof(routingKey));
            }

            if (IsShutdown == true)
            {
                throw new RabbitMQException("Cannot Unbind an Exchange from another Exchange since the Connection is Shutdown");
            }

            await _channelSemaphore.WaitAsync(cancellationToken);

            try
            {
                if (IsConnected == false)
                {
                    throw new RabbitMQException("Cannot Unbind an Exchange from another Exchange while the Connection is Unavailable");
                }

                _channel.ExchangeUnbind(destinationName, sourceName, routingKey);
            }
            finally
            {
                _channelSemaphore.Release();
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="queueName">The Name of the Queue to be Bound</param>
        /// <param name="exchangeName">The Name of the Exchange to Bind to</param>
        /// <param name="routingKey">The Routing Key for this Queue Binding</param>
        /// <param name="cancellationToken">A Cancellation Token that can be used to signal the Asynchronous Operation should be Cancelled</param>
        /// <returns>A Task that will Complete upon successfully Binding a Queue to an Exchange</returns>
        public async Task BindQueueToExchange(string queueName, string exchangeName, string routingKey, CancellationToken cancellationToken)
        {
            if (queueName == null)
            {
                throw new ArgumentNullException(nameof(queueName));
            }

            if (exchangeName == null)
            {
                throw new ArgumentNullException(nameof(exchangeName));
            }

            if (routingKey == null)
            {
                throw new ArgumentNullException(nameof(routingKey));
            }

            if (IsShutdown == true)
            {
                throw new RabbitMQException("Cannot Bind a Queue to an Exchange since the Connection is Shutdown");
            }

            await _channelSemaphore.WaitAsync(cancellationToken);

            try
            {
                if (IsConnected == false)
                {
                    throw new RabbitMQException("Cannot Bind a Queue to an Exchange while the Connection is Unavailable");
                }

                _channel.QueueBind(queueName, exchangeName, routingKey);
            }
            finally
            {
                _channelSemaphore.Release();
            }
        }

        /// <summary>
        /// Unbind a Queue from an Exchange
        /// </summary>
        /// <param name="queueName">The Name of the Queue to Unbind</param>
        /// <param name="exchangeName">The Name of the Exchange</param>
        /// <param name="routingKey">The Routing Key for this Queue Binding</param>
        /// <param name="cancellationToken">A Cancellation Token that can be used to signal the Asynchronous Operation should be Cancelled</param>
        /// <returns>A Task that will Complete upon successfully Unbinding a Queue from an Exchange</returns>
        public async Task UnbindQueueFromExchange(string queueName, string exchangeName, string routingKey, CancellationToken cancellationToken)
        {
            if (queueName == null)
            {
                throw new ArgumentNullException(nameof(queueName));
            }

            if (exchangeName == null)
            {
                throw new ArgumentNullException(nameof(exchangeName));
            }

            if (routingKey == null)
            {
                throw new ArgumentNullException(nameof(routingKey));
            }

            if (IsShutdown == true)
            {
                throw new RabbitMQException("Cannot Unbind a Queue from an Exchange since the Connection is Shutdown");
            }

            await _channelSemaphore.WaitAsync(cancellationToken);

            try
            {
                if (IsConnected == false)
                {
                    throw new RabbitMQException("Cannot Unbind a Queue from an Exchange while the Connection is Unavailable");
                }

                _channel.QueueUnbind(queueName, exchangeName, routingKey);
            }
            finally
            {
                _channelSemaphore.Release();
            }
        }

        /// <summary>
        /// Create a Consumer that will call the provided Async Method when a Message is Received from the specified Queue
        /// </summary>
        /// <param name="queueName">The Name of the Queue to Start Consuming</param>
        /// <param name="asyncMethod">The Async Method that will Handle Received Messages</param>
        /// <param name="cancellationToken">A Cancellation Token that can be used to signal the Asynchronous Operation should be Cancelled</param>
        /// <returns>A Task that will Complete upon successfully Creating a Queue Consumer</returns>
        public async Task CreateQueueConsumer(string queueName, ConsumerReceiveHandler asyncMethod, CancellationToken cancellationToken)
        {
            if (queueName == null)
            {
                throw new ArgumentNullException(nameof(queueName));
            }

            if (asyncMethod == null)
            {
                throw new ArgumentNullException(nameof(asyncMethod));
            }

            if (IsShutdown == true)
            {
                throw new RabbitMQException("Cannot Create a Queue Consumer since the Connection is Shutdown");
            }

            await _channelSemaphore.WaitAsync(cancellationToken);

            try
            {
                if (IsConnected == false)
                {
                    throw new RabbitMQException("Cannot Create a Queue Consumer while the Connection is Unavailable");
                }

                if (_consumer == null)
                {
                    throw new RabbitMQException("Cannot Create a Queue Consumer since the Consumer Handler is Null");
                }

                if (_consumerAsyncMethods.TryAdd(queueName, asyncMethod) == false)
                {
                    throw new RabbitMQException("Failed to Create the Queue Consumer - A Consumer with the same Name for this Queue is already Registered");
                }

                if (_channel.BasicConsume(queueName, false, queueName, _consumer) != queueName)
                {
                    _consumerAsyncMethods.TryRemove(queueName, out _);

                    throw new RabbitMQException("Failed to Create the Queue Consumer - The Broker Consumer Tag did not match the Client Consumer Tag");
                }
            }
            finally
            {
                _channelSemaphore.Release();
            }
        }

        /// <summary>
        /// Destroy a Queue Consumer
        /// </summary>
        /// <param name="queueName">The Name of the Queue to Stop Consuming</param>
        /// <param name="cancellationToken">A Cancellation Token that can be used to signal the Asynchronous Operation should be Cancelled</param>
        /// <returns>A Task that will Complete upon successfully Destroying a Queue Consumer</returns>
        public async Task DestroyQueueConsumer(string queueName, CancellationToken cancellationToken)
        {
            if (queueName == null)
            {
                throw new ArgumentNullException(nameof(queueName));
            }

            if (IsShutdown == true)
            {
                throw new RabbitMQException("Cannot Destroy a Queue Consumer since the Connection is Shutdown");
            }

            await _channelSemaphore.WaitAsync(cancellationToken);

            try
            {
                if (IsConnected == false)
                {
                    throw new RabbitMQException("Cannot Destroy a Queue Consumer while the Connection is Unavailable");
                }

                if (_consumer == null)
                {
                    throw new RabbitMQException("Cannot Destroy a Queue Consumer since the Consumer Handler is Null");
                }

                _channel.BasicCancel(queueName);
            }
            finally
            {
                _channelSemaphore.Release();
            }

            _consumerAsyncMethods.TryRemove(queueName, out _);
        }

        /// <summary>
        /// Register an Async Method to Handle Results for Message Publishing Attempts
        /// </summary>
        /// <param name="messageType">The Message Type this Method will Handle</param>
        /// <param name="asyncMethod">The Async Method to Handle Results</param>
        public void RegisterPublishResultHandler(string messageType, PublishResultHandler asyncMethod)
        {
            if (messageType == null)
            {
                throw new ArgumentNullException(nameof(messageType));
            }

            if (asyncMethod == null)
            {
                throw new ArgumentNullException(nameof(asyncMethod));
            }

            if (_publishResultsAsyncMethods.TryAdd(messageType, asyncMethod) == false)
            {
                throw new RabbitMQException("The Message Type '" + messageType + "' already has a Registered Publish Result Handler");
            }
        }

        /// <summary>
        /// Unregister an Async Method to Handle Results for Message Publishing Attempts
        /// </summary>
        /// <param name="messageType">The Message Type to Unregister</param>
        public void UnregisterPublishResultHandler(string messageType)
        {
            if (messageType == null)
            {
                throw new ArgumentNullException(nameof(messageType));
            }

            _publishResultsAsyncMethods.TryRemove(messageType, out _);
        }

        /// <summary>
        /// Send a Message to be Published by the RabbitMQ Broker
        /// </summary>
        /// <param name="message">The Message to be Published</param>
        /// <param name="cancellationToken">A Cancellation Token that can be used to signal the Asynchronous Operation should be Cancelled</param>
        /// <returns>A Task that will Complete when the Message has been Accepted and Queued for Sending</returns>
        public async ValueTask Publish(PublishMessage message, CancellationToken cancellationToken)
        {
            if(IsShutdown == true)
            {
                throw new RabbitMQException("Cannot Publish a Message since the Connection is Shutdown");
            }
            
            if (IsConnected == false)
            {
                throw new RabbitMQException("Cannot Publish a Message while the Connection is Unavailable");
            }

            if (_publishSemaphore.Wait(0, cancellationToken) == false)
            {
                await _publishSemaphore.WaitAsync(cancellationToken);
            }

            bool removeMessageOnException = false;

            try
            {
                if(_publishMessages.TryAdd(message.MessageID, message))
                {
                    removeMessageOnException = true;
                }

                message.Publish(_channel);
            }
            catch (AlreadyClosedException e)
            {
                if(removeMessageOnException)
                {
                    _publishMessages.TryRemove(message.MessageID, out _);
                }

                throw new RabbitMQException("Failed to Publish a Message - The Connection is Closed", e);
            }
            catch (Exception)
            {
                if (removeMessageOnException)
                {
                    _publishMessages.TryRemove(message.MessageID, out _);
                }

                throw;
            }
            finally
            {
                _publishSemaphore.Release();
            }

            _publishMessages.AddOrUpdate(message.MessageID, message, (oldKey, oldMessage) =>
            {
                return message;
            });
        }

        /// <summary>
        /// Try to Send a Message to be Published by the RabbitMQ Broker
        /// </summary>
        /// <param name="message">The Message to be Published</param>
        /// <param name="cancellationToken">A Cancellation Token that can be used to signal the Asynchronous Operation should be Cancelled</param>
        /// <returns>A Task that will Complete with a Result to indicate if the Message has been Accepted and Queued for Sending</returns>
        public async ValueTask<bool> TryPublish(PublishMessage message, CancellationToken cancellationToken)
        {
            if(IsShutdown == true || IsConnected == false)
            {
                return false;
            }

            try
            {
                await Publish(message, cancellationToken);

                return true;
            }
            catch
            {
                return false;
            }
        }

        #endregion


        #region Internal Methods

#if NETSTANDARD
        /// <summary>
        /// Send an Ack Response for one or more Delivery Tags to the RabbitMQ Broker
        /// </summary>
        /// <param name="deliveryTag">The Delivery Tag to Ack</param>
        /// <param name="multiple">Whether all Delivery Tags prior to and including this Tag should be Ack'd</param>
        /// <returns>A Task that will Complete upon successfully Sending an Ack Response to the RabbitMQ Broker</returns>
        internal Task SendAck(ulong deliveryTag, bool multiple = false)
        {
            if (IsShutdown == true)
            {
                throw new RabbitMQException("Cannot Ack a Message since the Connection is Shutdown");
            }

            if (IsConnected == false)
            {
                throw new RabbitMQException("Cannot Ack a Message while the Connection is Unavailable");
            }

            try
            {
                _channel.BasicAck(deliveryTag, multiple);
            }
            catch (AlreadyClosedException e)
            {
                throw new RabbitMQException("Failed to Ack a Message - The Connection is Closed", e);
            }

            return Task.CompletedTask;
        }
#else
        /// <summary>
        /// Send an Ack Response for one or more Delivery Tags to the RabbitMQ Broker
        /// </summary>
        /// <param name="deliveryTag">The Delivery Tag to Ack</param>
        /// <param name="multiple">Whether all Delivery Tags prior to and including this Tag should be Ack'd</param>
        /// <returns>A Task that will Complete upon successfully Sending an Ack Response to the RabbitMQ Broker</returns>
        internal ValueTask SendAck(ulong deliveryTag, bool multiple = false)
        {
            if (IsShutdown == true)
            {
                throw new RabbitMQException("Cannot Ack a Message since the Connection is Shutdown");
            }

            if (IsConnected == false)
            {
                throw new RabbitMQException("Cannot Ack a Message while the Connection is Unavailable");
            }

            try
            {
                _channel.BasicAck(deliveryTag, multiple);
            }
            catch (AlreadyClosedException e)
            {
                throw new RabbitMQException("Failed to Ack a Message - The Connection is Closed", e);
            }

            return ValueTask.CompletedTask;
        }
#endif

        /// <summary>
        /// Try to Send an Ack Response for one or more Delivery Tags to the RabbitMQ Broker
        /// </summary>
        /// <param name="deliveryTag">The Delivery Tag to Ack</param>
        /// <param name="multiple">Whether all Delivery Tags prior to and including this Tag should be Ack'd</param>
        /// <returns>A Task that will Complete with a Result to indicate if the Ack Response was Sent to the RabbitMQ Broker</returns>
        internal async ValueTask<bool> TrySendAck(ulong deliveryTag, bool multiple = false)
        {
            if (IsShutdown == true || IsConnected == false)
            {
                return false;
            }

            try
            {
                await SendAck(deliveryTag, multiple);

                return true;
            }
            catch
            {
                return false;
            }
        }

#if NETSTANDARD
        /// <summary>
        /// Send a Nack Response for one or more Delivery Tags to the RabbitMQ Broker
        /// </summary>
        /// <param name="deliveryTag">The Delivery Tag to Nack</param>
        /// <param name="requeue">Whether the Delivery Tag(s) should be Requeued for Delivery by the RabbitMQ Broker</param>
        /// <param name="multiple">Whether all Delivery Tags prior to and including this Tag should be Nack'd</param>
        /// <returns>A Task that will Complete upon successfully Sending a Nack Response to the RabbitMQ Broker</returns>
        internal Task SendNack(ulong deliveryTag, bool requeue, bool multiple = false)
        {
            if (IsShutdown == true)
            {
                throw new RabbitMQException("Cannot Nack a Message since the Connection is Shutdown");
            }

            if (IsConnected == false)
            {
                throw new RabbitMQException("Cannot Nack a Message while the Connection is Unavailable");
            }

            try
            {
                _channel.BasicNack(deliveryTag, multiple, requeue);
            }
            catch (AlreadyClosedException e)
            {
                throw new RabbitMQException("Failed to Nack a Message - The Connection is Closed", e);
            }

            return Task.CompletedTask;
        }
#else
        /// <summary>
        /// Send a Nack Response for one or more Delivery Tags to the RabbitMQ Broker
        /// </summary>
        /// <param name="deliveryTag">The Delivery Tag to Nack</param>
        /// <param name="requeue">Whether the Delivery Tag(s) should be Requeued for Delivery by the RabbitMQ Broker</param>
        /// <param name="multiple">Whether all Delivery Tags prior to and including this Tag should be Nack'd</param>
        /// <returns>A Task that will Complete upon successfully Sending a Nack Response to the RabbitMQ Broker</returns>
        internal ValueTask SendNack(ulong deliveryTag, bool requeue, bool multiple = false)
        {
            if (IsShutdown == true)
            {
                throw new RabbitMQException("Cannot Nack a Message since the Connection is Shutdown");
            }

            if (IsConnected == false)
            {
                throw new RabbitMQException("Cannot Nack a Message while the Connection is Unavailable");
            }

            try
            {
                _channel.BasicNack(deliveryTag, multiple, requeue);
            }
            catch (AlreadyClosedException e)
            {
                throw new RabbitMQException("Failed to Nack a Message - The Connection is Closed", e);
            }

            return ValueTask.CompletedTask;
        }
#endif

        /// <summary>
        /// Try to Send a Nack Response for one or more Delivery Tags to the RabbitMQ Broker
        /// </summary>
        /// <param name="deliveryTag">The Delivery Tag to Nack</param>
        /// <param name="requeue">Whether the Delivery Tag(s) should be Requeued for Delivery by the RabbitMQ Broker</param>
        /// <param name="multiple">Whether all Delivery Tags prior to and including this Tag should be Nack'd</param>
        /// <returns>A Task that will Complete with a Result to indicate if the Nack Response was Sent to the RabbitMQ Broker</returns>
        internal async ValueTask<bool> TrySendNack(ulong deliveryTag, bool requeue, bool multiple = false)
        {
            if (IsShutdown == true || IsConnected == false)
            {
                return false;
            }

            try
            {
                await SendNack(deliveryTag, requeue, multiple);

                return true;
            }
            catch
            {
                return false;
            }
        }

        #endregion


        #region Private Methods

        private async Task initializeConnection(CancellationToken cancellationToken)
        {
            if (_connection != null)
            {
                throw new RabbitMQException("The Connection was already Initialized");
            }

            cancellationToken.ThrowIfCancellationRequested();

            while (cancellationToken.IsCancellationRequested == false)
            {
                if (_connectionFactory == null)
                {
                    throw new RabbitMQException("The Connection Factory was Null during Connection Initialization");
                }

                TimeSpan connectionRetryInterval = _connectionFactory.NetworkRecoveryInterval;

                if (_servers == null)
                {
                    throw new RabbitMQException("The Servers Collection was Null during Connection Initialization");
                }

                try
                {
                    _connection = (IAutorecoveringConnection)_connectionFactory.CreateConnection(_servers.ToList());

                    if (_connection != null && _connection.IsOpen == true)
                    {
                        _connection.RecoverySucceeded += connectionRecoverySucceeded;
                        _connection.ConnectionRecoveryError += connectionRecoveryError;
                        _connection.CallbackException += connectionCallbackException;
                        _connection.ConnectionShutdown += connectionConnectionShutdown;

                        return;
                    }

                    destroyConnection();
                }
                catch
                {
                    destroyConnection();
                }

                await Task.Delay(connectionRetryInterval, cancellationToken);
            }

            if (cancellationToken.IsCancellationRequested)
            {
                destroyConnection();
            }

            cancellationToken.ThrowIfCancellationRequested();
        }

        private void destroyConnection()
        {
            if (_connection == null)
            {
                return;
            }

            _connection.RecoverySucceeded -= connectionRecoverySucceeded;
            _connection.ConnectionRecoveryError -= connectionRecoveryError;
            _connection.CallbackException -= connectionCallbackException;
            _connection.ConnectionShutdown -= connectionConnectionShutdown;

            try
            {
                _connection.Close(_connectionFactory?.ContinuationTimeout ?? TimeSpan.FromSeconds(4));
            }
            catch
            {
            }

            try
            {
                _connection.Dispose();
            }
            catch
            {
            }

            _connection = null;
        }

        private async Task initializeChannel(CancellationToken cancellationToken)
        {
            if (_channel != null)
            {
                throw new RabbitMQException("The Channel was already Initialized");
            }

            cancellationToken.ThrowIfCancellationRequested();

            while (cancellationToken.IsCancellationRequested == false)
            {
                if (_connectionFactory == null)
                {
                    throw new RabbitMQException("The Connection Factory was Null during Channel Initialization");
                }

                TimeSpan connectionRetryInterval = _connectionFactory.NetworkRecoveryInterval;

                if (_connection == null)
                {
                    throw new RabbitMQException("The Connection was Null during Channel Initialization");
                }

                if (_connection.IsOpen)
                {
                    await _channelSemaphore.WaitAsync(cancellationToken);

                    try
                    {
                        _channel = _connection.CreateModel();

                        if (_channel != null && _channel.IsOpen == true)
                        {
                            _channel.BasicAcks += channelReceivedAck;
                            _channel.BasicNacks += channelReceivedNack;
                            _channel.BasicReturn += channelReceivedReturn;
                            _channel.CallbackException += channelCallbackException;
                            _channel.ModelShutdown += channelModelShutdown;

                            ushort prefetchCount = 1;

                            if (_connectionFactory.ConsumerDispatchConcurrency > 1 && _connectionFactory.ConsumerDispatchConcurrency <= ushort.MaxValue)
                            {
                                prefetchCount = (ushort)_connectionFactory.ConsumerDispatchConcurrency;
                            }

                            _channel.BasicQos(0, prefetchCount, false);

                            _channel.ConfirmSelect();

                            _consumerCts = new CancellationTokenSource();

                            _consumer = new AsyncEventingBasicConsumer(_channel);

                            _consumer.Received += consumerReceived;

                            return;
                        }

                        destroyChannel();
                    }
                    catch
                    {
                        destroyChannel();
                    }
                    finally
                    {
                        _channelSemaphore.Release();
                    }
                }

                await Task.Delay(connectionRetryInterval, cancellationToken);
            }

            if (cancellationToken.IsCancellationRequested)
            {
                destroyChannel();
            }

            cancellationToken.ThrowIfCancellationRequested();
        }

        private void destroyChannel()
        {
            _consumerCts?.Cancel();

            if (_consumer != null)
            {
                _consumer.Received -= consumerReceived;
                _consumer = null;
            }

            if (_channel == null)
            {
                return;
            }

            _channel.BasicAcks -= channelReceivedAck;
            _channel.BasicNacks -= channelReceivedNack;
            _channel.BasicReturn -= channelReceivedReturn;
            _channel.CallbackException -= channelCallbackException;
            _channel.ModelShutdown -= channelModelShutdown;

            try
            {
                _channel.Close();
            }
            catch
            {
            }

            try
            {
                _channel.Dispose();
            }
            catch
            {
            }

            _channel = null;
        }

        private void connectionRecoverySucceeded(object sender, EventArgs e)
        {
            ConnectionRecoverySuccess?.Invoke(this);
        }

        private void connectionRecoveryError(object sender, ConnectionRecoveryErrorEventArgs e)
        {
            if(e == null || e.Exception == null || ConnectionRecoveryError == null)
            {
                return;
            }

            ConnectionRecoveryError(this, e.Exception);
        }

        private void connectionCallbackException(object sender, CallbackExceptionEventArgs e)
        {
            if (e == null || e.Exception == null || ConnectionException == null)
            {
                return;
            }

            if (e.Exception is ObjectDisposedException ode && ode.Message == "The semaphore has been disposed.")
            {
                // TODO: Remove this temporary code when Pull-Request https://github.com/rabbitmq/rabbitmq-dotnet-client/pull/1015 is Merged and Released
                return;
            }
            else if(e.Exception is AggregateException ae && ae.InnerExceptions.Any(exception => exception.Message == "The semaphore has been disposed."))
            {
                // TODO: Remove this temporary code when Pull-Request https://github.com/rabbitmq/rabbitmq-dotnet-client/pull/1015 is Merged and Released
                return;
            }

            ConnectionException(this, e.Exception);
        }

        private void connectionConnectionShutdown(object sender, ShutdownEventArgs e)
        {
            if (e == null || e.Initiator == ShutdownInitiator.Application || e.ReplyText == null || UnexpectedConnectionShutdown == null)
            {
                return;
            }

            if(e.ReplyCode == 0 || e.ReplyCode == 320)
            {
                return;
            }

            if(_connection.IsOpen == true) // Ignore Shutdown Errors when the Connection is still usable
            {
                return;
            }

            lock (_connectionShutdownLock)
            {
                if (_connectionShutdown == true)
                {
                    return;
                }

                _connectionShutdown = true;
            }

            UnexpectedConnectionShutdown(this, e.ReplyCode, e.ReplyText);
        }

        private void channelReceivedAck(object sender, BasicAckEventArgs e)
        {
            if (e == null)
            {
                return;
            }

            foreach (PublishMessage message in _publishMessages.Values.Where(message => message.DeliveryTag == e.DeliveryTag || (e.Multiple == true && message.DeliveryTag <= e.DeliveryTag)))
            {
                transitionPublishMessageToResult(message.MessageID, PublishResultType.Success);
            }
        }

        private void channelReceivedNack(object sender, BasicNackEventArgs e)
        {
            if (e == null)
            {
                return;
            }

            foreach (PublishMessage message in _publishMessages.Values.Where(message => message.DeliveryTag == e.DeliveryTag || (e.Multiple == true && message.DeliveryTag <= e.DeliveryTag)))
            {
                transitionPublishMessageToResult(message.MessageID, PublishResultType.BrokerError);
            }
        }

        private void channelReceivedReturn(object sender, BasicReturnEventArgs e)
        {
            if (e == null || e.BasicProperties == null || e.BasicProperties.MessageId == null || e.BasicProperties.MessageId.Length == 0)
            {
                return;
            }

            if (Guid.TryParse(e.BasicProperties.MessageId, out Guid messageId) == false)
            {
                return;
            }

            if(_publishMessages.ContainsKey(messageId) == false)
            {
                return;
            }

            transitionPublishMessageToResult(messageId, PublishResultType.Returned, e.ReplyCode, e.ReplyText);
        }

        private void channelCallbackException(object sender, CallbackExceptionEventArgs e)
        {
            if (e == null || e.Exception == null || ChannelException == null)
            {
                return;
            }

            ChannelException(this, e.Exception);
        }

        private void channelModelShutdown(object sender, ShutdownEventArgs e)
        {
            if(e == null || e.Initiator == ShutdownInitiator.Application || e.ReplyText == null || UnexpectedConnectionShutdown == null)
            {
                return;
            }

            if(e.ReplyCode == 0 || e.ReplyCode == 320)
            {
                return;
            }

            if (_channel.IsOpen == true) // Ignore Shutdown Errors when the Channel is still usable
            {
                return;
            }

            lock (_connectionShutdownLock)
            {
                if (_connectionShutdown == true)
                {
                    return;
                }

                _connectionShutdown = true;
            }

            UnexpectedConnectionShutdown(this, e.ReplyCode, e.ReplyText);
        }

        private async Task consumerReceived(object sender, BasicDeliverEventArgs eventArgs)
        {
            if(eventArgs == null)
            {
                return;
            }

            Guid? messageId = extractMessageId(eventArgs);

            if(messageId.HasValue)
            {
                trackConsumerRedeliveredMessage(messageId.Value, eventArgs);
            }

            if (_consumerAsyncMethods.TryGetValue(eventArgs.ConsumerTag, out ConsumerReceiveHandler asyncMethod) == false)
            {
                await handleNackForConsumerReceived(messageId, eventArgs);
                return;
            }

            ReceivedMessage receivedMessage = ReceivedMessage.CreateFromEvent(eventArgs);

            if (receivedMessage == null)
            {
                await handleNackForConsumerReceived(messageId, eventArgs);
                return;
            }

            if (_consumerCts.IsCancellationRequested)
            {
                await handleNackForConsumerReceived(messageId, eventArgs);
                return;
            }

            try
            {
                ConsumerResultType result = await asyncMethod(receivedMessage, _consumerCts.Token);

                if (result == ConsumerResultType.Accept)
                {
                    await TrySendAck(eventArgs.DeliveryTag);

                    // Remove the Tracked Message since we sent an Ack
                    if (messageId.HasValue)
                    {
                        _consumerRedeliveredMessages.TryRemove(messageId.Value, out _);
                    }
                }
                else if(result == ConsumerResultType.Discard)
                {
                    await handleNackForConsumerReceived(messageId, eventArgs, true);
                }
                else
                {
                    await handleNackForConsumerReceived(messageId, eventArgs);
                }
            }
            catch (AlreadyClosedException)
            {
                await handleNackForConsumerReceived(messageId, eventArgs);
            }
            catch (Exception e)
            {
                ConsumerException?.Invoke(this, e);

                await handleNackForConsumerReceived(messageId, eventArgs);
            }
        }

        private async Task handleNackForConsumerReceived(Guid? messageId, BasicDeliverEventArgs eventArgs, bool forceNoRequeue = false)
        {
            try
            {
                if (eventArgs.DeliveryTag == 0)
                {
                    return;
                }

                if (eventArgs.Redelivered == false)
                {
                    await TrySendNack(eventArgs.DeliveryTag, true);
                    return;
                }

                if (messageId.HasValue == false || messageId.Value == Guid.Empty)
                {
                    // Nack without Redelivering as we cannot track how many times this message has been redelivered
                    await TrySendNack(eventArgs.DeliveryTag, false);
                    return;
                }

                bool redeliver = !forceNoRequeue;
                TimeSpan nackDelay = TimeSpan.Zero;

                if (forceNoRequeue == false && _consumerRedeliveredMessages.TryGetValue(messageId.Value, out RedeliveredMessage message))
                {
                    if(message.LastTimestamp.Subtract(message.FirstTimestamp).TotalHours >= 3 && message.RedeliveredCount >= 25)
                    {
                        redeliver = false;
                    }
                    else if (message.RedeliveredCount > 1 && message.LastTimestamp.Subtract(message.FirstTimestamp).TotalMinutes <= 10)
                    {
                        nackDelay = TimeSpan.FromMinutes(1);
                    }
                    else if (message.RedeliveredCount > 1 && message.LastTimestamp.Subtract(message.FirstTimestamp).TotalMinutes > 10)
                    {
                        nackDelay = TimeSpan.FromMinutes(5);
                    }
                }

                if(redeliver == true && nackDelay > TimeSpan.Zero)
                {
                    _ = Task.Run(async () =>
                    {
                        await Task.Delay(nackDelay);

                        await TrySendNack(eventArgs.DeliveryTag, true);
                    });
                }
                else
                {
                    await TrySendNack(eventArgs.DeliveryTag, redeliver);
                }

                if (redeliver == false)
                {
                    // Remove the Tracked Message as we expect it won't be redelivered again
                    _consumerRedeliveredMessages.TryRemove(messageId.Value, out _);
                }
            }
            catch (AlreadyClosedException)
            {
            }
            catch (Exception e)
            {
                ConsumerException?.Invoke(this, e);
            }
        }

        private void trackConsumerRedeliveredMessage(Guid messageId, BasicDeliverEventArgs eventArgs)
        {
            foreach (RedeliveredMessage staleMessage in _consumerRedeliveredMessages.Values.Where(message => DateTime.UtcNow.Subtract(message.LastTimestamp).TotalHours >= 2))
            {
                _consumerRedeliveredMessages.TryRemove(staleMessage.MessageID, out _);
            }

            if(eventArgs.Redelivered == false)
            {
                return;
            }

            _consumerRedeliveredMessages.AddOrUpdate(messageId, new RedeliveredMessage
            {
                MessageID = messageId,
                FirstTimestamp = DateTime.UtcNow,
                LastTimestamp = DateTime.UtcNow,
                RedeliveredCount = 1,
            }, (existingKey, existingMessage) =>
            {
                existingMessage.LastTimestamp = DateTime.UtcNow;
                existingMessage.RedeliveredCount++;

                return existingMessage;
            });
        }

        private async Task publishMessagesHandler()
        {
            CancellationToken cancellationToken = _publishMessagesCts.Token;

            try
            {
                while(cancellationToken.IsCancellationRequested == false)
                {
                    foreach(PublishMessage message in _publishMessages.Values.Where(message => DateTime.UtcNow.Subtract(message.PublishTimestamp) >= message.PublishTimeout))
                    {
                        try
                        {
                            if (message.HasRetriesAvailable)
                            {
                                if(await TryPublish(message, cancellationToken) == false)
                                {
                                    message.UpdateFailedPublish();
                                }
                            }
                            else
                            {
                                transitionPublishMessageToResult(message.MessageID, PublishResultType.Timeout);
                            }
                        }
                        catch (OperationCanceledException)
                        {
                            throw;
                        }
                        catch (Exception e)
                        {
                            PublishException?.Invoke(this, e);
                        }
                    }

                    await Task.Delay(100);
                }
            }
            catch (OperationCanceledException)
            {
            }
        }

        private void transitionPublishMessageToResult(Guid messageId, PublishResultType result, int? failureCode = null, string failureReason = null)
        {
            if(_publishMessages.TryRemove(messageId, out PublishMessage message))
            {
                try
                {
                    _publishResultsChannel.Writer.TryWrite(new PublishResult
                    {
                        MessageID = messageId,
                        MessageType = message.Type,
                        Result = result,
                        FailureCode = failureCode,
                        FailureReason = failureReason,
                    });
                }
                catch (Exception e)
                {
                    PublishException?.Invoke(this, e);
                }
            }
        }

        private async Task publishResultsHandler()
        {
            CancellationToken cancellationToken = _publishResultsCts.Token;

            try
            {
                while(await _publishResultsChannel.Reader.WaitToReadAsync(cancellationToken))
                {
                    while(_publishResultsChannel.Reader.TryRead(out PublishResult publishResult))
                    {
                        if (publishResult.MessageType != null && _publishResultsAsyncMethods.TryGetValue(publishResult.MessageType, out PublishResultHandler asyncMethod))
                        {
                            if (_publishResultsSemaphore.Wait(0) == false)
                            {
                                await _publishResultsSemaphore.WaitAsync(cancellationToken);
                            }

                            cancellationToken.ThrowIfCancellationRequested();

                            _ = publishResultAsync(publishResult, asyncMethod, _publishResultsSemaphore, cancellationToken);
                        }

                        cancellationToken.ThrowIfCancellationRequested();
                    }

                    cancellationToken.ThrowIfCancellationRequested();
                }
            }
            catch (OperationCanceledException)
            {
            }
        }

        private async Task publishResultAsync(PublishResult result, PublishResultHandler asyncMethod, SemaphoreSlim semaphore, CancellationToken cancellationToken)
        {
            try
            {
                Task asyncTask = asyncMethod(result.MessageID, result.Result, result.FailureCode, result.FailureReason, cancellationToken);

                if(asyncTask.IsCompleted == false)
                {
                    await asyncTask;
                }
            }
            catch (OperationCanceledException)
            {
            }
            catch (Exception e)
            {
                PublishException?.Invoke(this, e);
            }
            finally
            {
                semaphore.Release();
            }
        }

        private static Guid? extractMessageId(BasicDeliverEventArgs eventArgs)
        {
            if (eventArgs == null || eventArgs.BasicProperties == null || eventArgs.BasicProperties.IsMessageIdPresent() == false)
            {
                return null;
            }

            if (Guid.TryParse(eventArgs.BasicProperties.MessageId, out Guid messageId) && messageId != Guid.Empty)
            {
                return messageId;
            }

            return null;
        }

        #endregion


        #region Events

        /// <summary>
        /// An Event that is Raised when an Exception occurs in a RabbitMQ Connection
        /// </summary>
        public event ExceptionEventHandler ConnectionException;

        /// <summary>
        /// An Event that is Raised when an Exception occurs in a RabbitMQ Channel
        /// </summary>
        public event ExceptionEventHandler ChannelException;

        /// <summary>
        /// An Event that is Raised when an Exception occurs in a RabbitMQ Consumer
        /// </summary>
        public event ExceptionEventHandler ConsumerException;

        /// <summary>
        /// An Event that is Raised when an Exception occurs during a RabbitMQ Publish Attempt
        /// </summary>
        public event ExceptionEventHandler PublishException;

        /// <summary>
        /// An Event that is Raised when the RabbitMQ Connection Recovers Successfully
        /// </summary>
        public event ConnectionRecoverySuccessHandler ConnectionRecoverySuccess;

        /// <summary>
        /// An Event that is Raised when the RabbitMQ Connection Fails to Recover with an Exception
        /// </summary>
        public event ConnectionRecoveryErrorHandler ConnectionRecoveryError;

        /// <summary>
        /// An Event that is Raised when the RabbitMQ Connection is unexpectedly Shutdown and is no longer useable
        /// </summary>
        public event UnexpectedConnectionShutdownHandler UnexpectedConnectionShutdown;

        #endregion
    }
}

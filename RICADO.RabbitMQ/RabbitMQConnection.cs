using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace RICADO.RabbitMQ
{
    public sealed class RabbitMQConnection : IRabbitMQConnection
    {
        #region Constants

        internal const int DefaultConnectionTimeout = 2000;
        internal const int DefaultConnectionRecoveryInterval = 2000;
        internal const int DefaultHeartbeatInterval = 5000;
        internal const int DefaultReadTimeout = 4000;
        internal const int DefaultWriteTimeout = 2000;

        #endregion


        #region Private Fields

        private readonly string _connectionName;
        private readonly string _applicationName;
        private readonly string _username;
        private readonly string _password;
        private readonly string _virtualHost;
        private readonly ICollection<string> _servers;

        private ConnectionFactory _connectionFactory;
        private IAutorecoveringConnection _connection;

        private bool _connectionShutdown = false;
        private readonly object _connectionShutdownLock = new object();

        #endregion


        #region Public Properties

        public string ConnectionName => _connectionName;

        public string ApplicationName => _applicationName;

        public string Username => _username;

        public string Password => _password;

        public string VirtualHost => _virtualHost;

        public ICollection<string> Servers => _servers;

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

        public bool IsConnected => _connection?.IsOpen ?? false;

        public bool IsShutdown
        {
            get
            {
                lock (_connectionShutdownLock)
                {
                    return _connectionShutdown;
                }
            }
            private set
            {
                lock (_connectionShutdownLock)
                {
                    _connectionShutdown = value;
                }
            }
        }

        #endregion


        #region Constructor

        /// <summary>
        /// Create a new <see cref="RabbitMQConnection"/> Instance
        /// </summary>
        /// <param name="connectionName">The Name for this Connection (shown in the RabbitMQ Management UI)</param>
        /// <param name="applicationName">The Name of the Application that is utilizing this Connection</param>
        /// <param name="username">The Username for Authentication with the RabbitMQ Broker</param>
        /// <param name="password">The Password for Authentication with RabbitMQ Broker</param>
        /// <param name="virtualHost">The VirtualHost to utilize on the RabbitMQ Broker</param>
        /// <param name="servers">A Collection of DNS Names or IP Addresses for RabbitMQ Brokers</param>
        /// <param name="useSsl">Whether to use a TLS or Non-TLS Connection</param>
        /// <param name="sslCommonName">The Common Name (CN) of the SSL Certificate used by the RabbitMQ Broker</param>
        public RabbitMQConnection(string connectionName, string applicationName, string username, string password, string virtualHost, ICollection<string> servers, bool useSsl = false, string sslCommonName = null)
        {
            if (connectionName == null)
            {
                throw new ArgumentNullException(nameof(connectionName));
            }

            if (connectionName.Length == 0)
            {
                throw new ArgumentOutOfRangeException(nameof(connectionName), "The Connection Name cannot be Empty");
            }

            _connectionName = connectionName;

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
                    ClientProvidedName = _connectionName,
                    UserName = _username,
                    Password = _password,
                    VirtualHost = _virtualHost,

                    AutomaticRecoveryEnabled = true,
                    TopologyRecoveryEnabled = true,
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

                if (useSsl == true)
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

        public async Task Initialize(CancellationToken cancellationToken)
        {
            IsShutdown = false;

            await initializeConnection(cancellationToken);
        }

        public Task Destroy(CancellationToken cancellationToken)
        {
            IsShutdown = true;

            destroyConnection();

            _connectionFactory = null;

            return Task.CompletedTask;
        }

        public IRabbitMQConsumerChannel CreateConsumerChannel()
        {
            return new RabbitMQConsumerChannel(this);
        }

        public IRabbitMQPublisherChannel CreatePublisherChannel()
        {
#if NETSTANDARD
            TimeSpan defaultPublishTimeout = TimeSpan.FromMilliseconds(HeartbeatInterval.TotalMilliseconds * 2);
#else
            TimeSpan defaultPublishTimeout = HeartbeatInterval.Multiply(2);
#endif
            return new RabbitMQPublisherChannel(this, defaultPublishTimeout);
        }

        #endregion


        #region Internal Methods

        internal IModel CreateUnderlyingChannel()
        {
            return _connection.CreateModel();
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

        private void connectionRecoverySucceeded(object sender, EventArgs e)
        {
            ConnectionRecovered?.Invoke(this);
        }

        private void connectionRecoveryError(object sender, ConnectionRecoveryErrorEventArgs e)
        {
            if (e == null || e.Exception == null || ConnectionRecoveryError == null)
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

            ConnectionException(this, e.Exception);
        }

        private void connectionConnectionShutdown(object sender, ShutdownEventArgs e)
        {
            if (e == null || e.Initiator == ShutdownInitiator.Application || e.ReplyText == null)
            {
                return;
            }

            if (e.ReplyCode == 0)
            {
                return;
            }

            ConnectionLost?.Invoke(this, e.ReplyCode, e.ReplyText);
        }

        #endregion


        #region Events

        public event ConnectionExceptionHandler ConnectionException;

        public event ConnectionRecoveredHandler ConnectionRecovered;

        public event ConnectionLostHandler ConnectionLost;

        public event ConnectionRecoveryErrorHandler ConnectionRecoveryError;

        #endregion
    }
}

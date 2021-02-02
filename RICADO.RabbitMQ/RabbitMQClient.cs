using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Channels;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;
using RabbitMQ.Client.Events;
using Newtonsoft.Json.Linq;
using RICADO.Logging;

namespace RICADO.RabbitMQ
{
    public class RabbitMQClient
    {
        #region Constants

        internal const int DEFAULT_CONNECTION_TIMEOUT = 2000;
        internal const int DEFAULT_CONNECTION_RECOVERY_INTERVAL = 2000;
        internal const int DEFAULT_HEART_BEAT_INTERVAL = 5000;
        internal const int DEFAULT_READ_TIMEOUT = 4000;
        internal const int DEFAULT_WRITE_TIMEOUT = 2000;

        #endregion


        #region Private Properties

        private string _clientName;
        private string _applicationName;
        private string _username;
        private string _password;
        private string _virtualHost;
        private ICollection<string> _servers;

        private ConnectionFactory _connectionFactory;
        private IAutorecoveringConnection _connection;
        private IModel _channel;
        private AsyncEventingBasicConsumer _consumer;
        private CancellationTokenSource _consumerCts;

        private ConcurrentDictionary<string, Func<ReceivedMessage, CancellationToken, Task>> _consumerAsyncMethods = new ConcurrentDictionary<string, Func<ReceivedMessage, CancellationToken, Task>>();
        private ConcurrentDictionary<Guid, RedeliveredMessage> _consumerRedeliveredMessages = new ConcurrentDictionary<Guid, RedeliveredMessage>();

        private SemaphoreSlim _channelSemaphore = new SemaphoreSlim(1, 1);

        private ConcurrentDictionary<Guid, PublishMessage> _publishMessages = new ConcurrentDictionary<Guid, PublishMessage>();
        private SemaphoreSlim _publishSemaphore = new SemaphoreSlim(1, 1);
        private Task _publishMessagesTask;
        private CancellationTokenSource _publishMessagesCts;

        private ConcurrentDictionary<string, Func<Guid, enPublishResult, CancellationToken, Task>> _publishResultsAsyncMethods = new ConcurrentDictionary<string, Func<Guid, enPublishResult, CancellationToken, Task>>();
        private Channel<PublishResult> _publishResultsChannel;
        private SemaphoreSlim _publishResultsSemaphore;
        private CancellationTokenSource _publishResultsCts;
        private Task _publishResultsTask;

        private TimeSpan? _defaultPublishTimeout;
        private int _defaultPublishRetries = 3;

        private ulong _lastBrokerDeliveryTag = 0;
        private object _lastBrokerDeliveryTagLock = new object();

        #endregion


        #region Internal Properties

        internal ulong LastBrokerDeliveryTag
        {
            get
            {
                lock (_lastBrokerDeliveryTagLock)
                {
                    return _lastBrokerDeliveryTag;
                }
            }
            set
            {
                lock (_lastBrokerDeliveryTagLock)
                {
                    _lastBrokerDeliveryTag = value;
                }
            }
        }

        #endregion


        #region Public Properties

        public string ClientName
        {
            get
            {
                return _clientName;
            }
        }

        public string ApplicationName
        {
            get
            {
                return _applicationName;
            }
        }

        public string Username
        {
            get
            {
                return _username;
            }
        }

        public string Password
        {
            get
            {
                return _password;
            }
        }

        public string VirtualHost
        {
            get
            {
                return _virtualHost;
            }
        }

        public ICollection<string> Servers
        {
            get
            {
                return _servers;
            }
        }

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

        public bool IsConnected => _channel?.IsOpen ?? false;

        #endregion


        #region Constructor

        public RabbitMQClient(string clientName, string applicationName, string username, string password, string virtualHost, ICollection<string> servers)
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

                    ContinuationTimeout = TimeSpan.FromSeconds(6),
                    HandshakeContinuationTimeout = TimeSpan.FromSeconds(6),
                    NetworkRecoveryInterval = TimeSpan.FromSeconds(2),
                    RequestedConnectionTimeout = TimeSpan.FromSeconds(2),
                    RequestedHeartbeat = TimeSpan.FromSeconds(5),
                    SocketReadTimeout = TimeSpan.FromSeconds(4),
                    SocketWriteTimeout = TimeSpan.FromSeconds(2),
                };
            }
            catch (Exception e)
            {
                throw new RabbitMQException("Unexpected Connection Factory Exception", e);
            }
        }

        public RabbitMQClient(string clientName, string applicationName, string username, string password, string virtualHost, ICollection<string> servers, string sslCommonName)
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

            if (sslCommonName == null)
            {
                throw new ArgumentNullException(nameof(sslCommonName));
            }

            if (sslCommonName.Length == 0)
            {
                throw new ArgumentOutOfRangeException(nameof(sslCommonName), "The SSL Common Name cannot be Empty");
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

                    Port = AmqpTcpEndpoint.DefaultAmqpSslPort,
                    AmqpUriSslProtocols = System.Security.Authentication.SslProtocols.Tls12,
                    Ssl = new SslOption()
                    {
                        Enabled = true,
                        ServerName = sslCommonName,
                        Version = System.Security.Authentication.SslProtocols.Tls12,
                    },

                    ContinuationTimeout = TimeSpan.FromSeconds(6),
                    HandshakeContinuationTimeout = TimeSpan.FromSeconds(6),
                    NetworkRecoveryInterval = TimeSpan.FromSeconds(2),
                    RequestedConnectionTimeout = TimeSpan.FromSeconds(2),
                    RequestedHeartbeat = TimeSpan.FromSeconds(5),
                    SocketReadTimeout = TimeSpan.FromSeconds(4),
                    SocketWriteTimeout = TimeSpan.FromSeconds(2),
                };
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
            await initializeConnection(cancellationToken);

            await initializeChannel(cancellationToken);

            _publishResultsChannel = Channel.CreateUnbounded<PublishResult>(new UnboundedChannelOptions { SingleReader = true, SingleWriter = false, AllowSynchronousContinuations = false });
            _publishResultsSemaphore = new SemaphoreSlim(ConsumerConcurrency, ConsumerConcurrency);
            _publishResultsCts = new CancellationTokenSource();
            _publishResultsTask = Task.Run(publishResultsHandler);

            _publishMessagesCts = new CancellationTokenSource();
            _publishMessagesTask = Task.Run(publishMessagesHandler);
        }

        public async Task Destroy(CancellationToken cancellationToken)
        {
            _publishResultsChannel?.Writer?.Complete();

            _publishResultsCts?.Cancel();

            _publishMessagesCts?.Cancel();

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

        public async Task DeclareExchange(string name, enExchangeType type, bool durable, bool autoDelete, CancellationToken cancellationToken)
        {
            if (name == null)
            {
                throw new ArgumentNullException(nameof(name));
            }

            await _channelSemaphore.WaitAsync(cancellationToken);

            try
            {
                if (_channel == null || _channel.IsOpen == false)
                {
                    throw new RabbitMQException("Cannot Declare an Exchange while the Channel is Unavailable");
                }

                _channel.ExchangeDeclare(name, type.ToString().ToLower(), durable, autoDelete);
            }
            finally
            {
                _channelSemaphore.Release();
            }
        }

        public async Task DeleteExchange(string name, bool ifUnused, CancellationToken cancellationToken)
        {
            if (name == null)
            {
                throw new ArgumentNullException(nameof(name));
            }

            await _channelSemaphore.WaitAsync(cancellationToken);

            try
            {
                if (_channel == null || _channel.IsOpen == false)
                {
                    throw new RabbitMQException("Cannot Delete an Exchange while the Channel is Unavailable");
                }

                _channel.ExchangeDelete(name, ifUnused);
            }
            finally
            {
                _channelSemaphore.Release();
            }
        }

        public async Task DeclareQueue(string name, bool durable, bool exclusive, bool autoDelete, CancellationToken cancellationToken, string deadLetterExchangeName = null)
        {
            if (name == null)
            {
                throw new ArgumentNullException(nameof(name));
            }

            await _channelSemaphore.WaitAsync(cancellationToken);

            try
            {
                if (_channel == null || _channel.IsOpen == false)
                {
                    throw new RabbitMQException("Cannot Declare a Queue while the Channel is Unavailable");
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

        public async Task<uint> DeleteQueue(string name, bool ifUnused, bool ifEmpty, CancellationToken cancellationToken)
        {
            if (name == null)
            {
                throw new ArgumentNullException(nameof(name));
            }

            await _channelSemaphore.WaitAsync(cancellationToken);

            try
            {
                if (_channel == null || _channel.IsOpen == false)
                {
                    throw new RabbitMQException("Cannot Delete a Queue while the Channel is Unavailable");
                }

                return _channel.QueueDelete(name, ifUnused, ifEmpty);
            }
            finally
            {
                _channelSemaphore.Release();
            }
        }

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

            await _channelSemaphore.WaitAsync(cancellationToken);

            try
            {
                if (_channel == null || _channel.IsOpen == false)
                {
                    throw new RabbitMQException("Cannot Bind an Exchange to another Exchange while the Channel is Unavailable");
                }

                _channel.ExchangeBind(destinationName, sourceName, routingKey);
            }
            finally
            {
                _channelSemaphore.Release();
            }
        }

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

            await _channelSemaphore.WaitAsync(cancellationToken);

            try
            {
                if (_channel == null || _channel.IsOpen == false)
                {
                    throw new RabbitMQException("Cannot Unbind an Exchange from another Exchange while the Channel is Unavailable");
                }

                _channel.ExchangeUnbind(destinationName, sourceName, routingKey);
            }
            finally
            {
                _channelSemaphore.Release();
            }
        }

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

            await _channelSemaphore.WaitAsync(cancellationToken);

            try
            {
                if (_channel == null || _channel.IsOpen == false)
                {
                    throw new RabbitMQException("Cannot Bind a Queue to an Exchange while the Channel is Unavailable");
                }

                _channel.QueueBind(queueName, exchangeName, routingKey);
            }
            finally
            {
                _channelSemaphore.Release();
            }
        }

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

            await _channelSemaphore.WaitAsync(cancellationToken);

            try
            {
                if (_channel == null || _channel.IsOpen == false)
                {
                    throw new RabbitMQException("Cannot Unbind a Queue from an Exchange while the Channel is Unavailable");
                }

                _channel.QueueUnbind(queueName, exchangeName, routingKey);
            }
            finally
            {
                _channelSemaphore.Release();
            }
        }

        public async Task CreateQueueConsumer(string queueName, Func<ReceivedMessage, CancellationToken, Task> asyncMethod, CancellationToken cancellationToken)
        {
            if (queueName == null)
            {
                throw new ArgumentNullException(nameof(queueName));
            }

            if (asyncMethod == null)
            {
                throw new ArgumentNullException(nameof(asyncMethod));
            }

            await _channelSemaphore.WaitAsync(cancellationToken);

            try
            {
                if (_channel == null || _channel.IsOpen == false)
                {
                    throw new RabbitMQException("Cannot Create a Queue Consumer while the Channel is Unavailable");
                }

                if (_consumer == null)
                {
                    throw new RabbitMQException("Cannot Create a Queue Consumer while the Consumer Handler is Null");
                }

                if (_consumerAsyncMethods.TryAdd(queueName, asyncMethod) == false)
                {
                    throw new RabbitMQException("Failed to Create the Queue Consumer - A Consumer with the same Name for this Queue is already Registered");
                }

                if (_channel.BasicConsume(queueName, false, queueName, _consumer) != queueName)
                {
                    Func<ReceivedMessage, CancellationToken, Task> removedMethod;

                    _consumerAsyncMethods.TryRemove(queueName, out removedMethod);

                    throw new RabbitMQException("Failed to Create the Queue Consumer - The Broker Consumer Tag did not match the Client Consumer Tag");
                }
            }
            finally
            {
                _channelSemaphore.Release();
            }
        }

        public async Task DestroyQueueConsumer(string queueName, CancellationToken cancellationToken)
        {
            if (queueName == null)
            {
                throw new ArgumentNullException(nameof(queueName));
            }

            await _channelSemaphore.WaitAsync(cancellationToken);

            try
            {
                if (_channel == null || _channel.IsOpen == false)
                {
                    throw new RabbitMQException("Cannot Create a Queue Consumer while the Channel is Unavailable");
                }

                if (_consumer == null)
                {
                    throw new RabbitMQException("Cannot Create a Queue Consumer while the Consumer Handler is Null");
                }

                _channel.BasicCancel(queueName);
            }
            finally
            {
                _channelSemaphore.Release();
            }

            Func<ReceivedMessage, CancellationToken, Task> removedMethod;

            _consumerAsyncMethods.TryRemove(queueName, out removedMethod);
        }

        public void RegisterPublishResultHandler(string messageType, Func<Guid, enPublishResult, CancellationToken, Task> asyncMethod)
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

        public void UnregisterPublishResultHandler(string messageType)
        {
            if (messageType == null)
            {
                throw new ArgumentNullException(nameof(messageType));
            }

            Func<Guid, enPublishResult, CancellationToken, Task> removedMethod;

            _publishResultsAsyncMethods.TryRemove(messageType, out removedMethod);
        }

        public async ValueTask Publish(PublishMessage message, CancellationToken cancellationToken)
        {
            if (_channel == null || _channel.IsOpen == false)
            {
                throw new RabbitMQException("Cannot Publish a Message while the Channel is Unavailable");
            }

            if (_publishSemaphore.Wait(0) == false)
            {
                await _publishSemaphore.WaitAsync(cancellationToken);
            }

            try
            {
                message.Publish(_channel);
            }
            catch (AlreadyClosedException e)
            {
                throw new RabbitMQException("Failed to Publish a Message - The Channel or Connection is Closed", e);
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

        public async ValueTask<bool> TryPublish(PublishMessage message, CancellationToken cancellationToken)
        {
            if(_channel == null || _channel.IsOpen == false)
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

        public ValueTask SendAck(ulong deliveryTag, bool multiple = false)
        {
            if(_channel == null || _channel.IsOpen == false)
            {
                throw new RabbitMQException("Cannot Ack a Message while the Channel is Unavailable");
            }

            try
            {
                _channel.BasicAck(deliveryTag, multiple);
            }
            catch (AlreadyClosedException e)
            {
                throw new RabbitMQException("Failed to Ack a Message - The Channel or Connection is Closed", e);
            }

            return ValueTask.CompletedTask;
        }

        public async ValueTask<bool> TrySendAck(ulong deliveryTag, bool multiple = false)
        {
            if(_channel == null || _channel.IsOpen == false)
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

        public ValueTask SendNack(ulong deliveryTag, bool requeue, bool multiple = false)
        {
            if (_channel == null || _channel.IsOpen == false)
            {
                throw new RabbitMQException("Cannot Nack a Message while the Channel is Unavailable");
            }

            try
            {
                _channel.BasicNack(deliveryTag, multiple, requeue);
            }
            catch (AlreadyClosedException e)
            {
                throw new RabbitMQException("Failed to Nack a Message - The Channel or Connection is Closed", e);
            }

            return ValueTask.CompletedTask;
        }

        public async ValueTask<bool> TrySendNack(ulong deliveryTag, bool requeue, bool multiple = false)
        {
            if (_channel == null || _channel.IsOpen == false)
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

                            ushort prefetchCount = 1;

                            if (_connectionFactory.ConsumerDispatchConcurrency > 1 && _connectionFactory.ConsumerDispatchConcurrency <= ushort.MaxValue)
                            {
                                prefetchCount = (ushort)_connectionFactory.ConsumerDispatchConcurrency;
                            }

                            _channel.BasicQos(0, prefetchCount, true);

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
            Console.WriteLine("Connection Recovery Succeeded for Client: {0}", _clientName);
        }

        private void connectionRecoveryError(object sender, ConnectionRecoveryErrorEventArgs e)
        {
            Console.WriteLine("Connection Recovery Error for Client: {0}", _clientName);

            if(e.Exception != null)
            {
                Console.WriteLine("Connection Recovery Exception: {0}", e.Exception);
            }
        }

        private void connectionCallbackException(object sender, CallbackExceptionEventArgs e)
        {
            if (e == null || e.Exception == null || ConnectionException == null)
            {
                return;
            }

            ConnectionException(e.Exception);
        }

        private void channelReceivedAck(object sender, BasicAckEventArgs e)
        {
            if (e == null)
            {
                return;
            }

            ulong firstDeliveryTag = e.DeliveryTag;
            ulong lastDeliveryTag = e.DeliveryTag;

            if(e.Multiple == true)
            {
                if(LastBrokerDeliveryTag > 0 && e.DeliveryTag > LastBrokerDeliveryTag)
                {
                    firstDeliveryTag = LastBrokerDeliveryTag;
                }
                else if(LastBrokerDeliveryTag == 0)
                {
                    firstDeliveryTag = 1;
                }
            }

            LastBrokerDeliveryTag = e.DeliveryTag;

            foreach (PublishMessage message in _publishMessages.Values.Where(message => message.DeliveryTag >= firstDeliveryTag && message.DeliveryTag <= lastDeliveryTag))
            {
                transitionPublishMessageToResult(message.MessageID, enPublishResult.Success);
            }
        }

        private void channelReceivedNack(object sender, BasicNackEventArgs e)
        {
            if (e == null)
            {
                return;
            }

            ulong firstDeliveryTag = e.DeliveryTag;
            ulong lastDeliveryTag = e.DeliveryTag;

            if (e.Multiple == true)
            {
                if (LastBrokerDeliveryTag > 0 && e.DeliveryTag > LastBrokerDeliveryTag)
                {
                    firstDeliveryTag = LastBrokerDeliveryTag;
                }
                else if (LastBrokerDeliveryTag == 0)
                {
                    firstDeliveryTag = 1;
                }
            }

            LastBrokerDeliveryTag = e.DeliveryTag;

            foreach (PublishMessage message in _publishMessages.Values.Where(message => message.DeliveryTag >= firstDeliveryTag && message.DeliveryTag <= lastDeliveryTag))
            {
                transitionPublishMessageToResult(message.MessageID, enPublishResult.Success);
            }
        }

        private void channelReceivedReturn(object sender, BasicReturnEventArgs e)
        {
            if (e == null || e.BasicProperties == null || e.BasicProperties.MessageId == null || e.BasicProperties.MessageId.Length == 0)
            {
                return;
            }

            Guid messageId;

            if (Guid.TryParse(e.BasicProperties.MessageId, out messageId) == false)
            {
                return;
            }

            if(_publishMessages.ContainsKey(messageId) == false)
            {
                return;
            }

            transitionPublishMessageToResult(messageId, enPublishResult.Returned);

            // TODO: Consider how we can capture and pass on the Reply Text and Reply Code to the Result Async Method
        }

        private void channelCallbackException(object sender, CallbackExceptionEventArgs e)
        {
            if (e == null || e.Exception == null || ChannelException == null)
            {
                return;
            }

            ChannelException(e.Exception);
        }

        private async Task consumerReceived(object sender, BasicDeliverEventArgs eventArgs)
        {
            // TODO: Move this logic to another Task or similar that can run every so often
            foreach(RedeliveredMessage message in _consumerRedeliveredMessages.Values.Where(message => DateTime.Now.Subtract(message.LastTimestamp).TotalMinutes >= 5))
            {
                RedeliveredMessage removedMessage;

                _consumerRedeliveredMessages.TryRemove(message.MessageID, out removedMessage);
            }
            
            if(eventArgs == null)
            {
                return;
            }

            Func<ReceivedMessage, CancellationToken, Task> asyncMethod;

            if (_consumerAsyncMethods.TryGetValue(eventArgs.ConsumerTag, out asyncMethod) == false)
            {
                await handleNackForConsumerReceived(eventArgs);
                return;
            }

            ReceivedMessage receivedMessage = ReceivedMessage.CreateFromEvent(eventArgs);

            if (receivedMessage == null)
            {
                await handleNackForConsumerReceived(eventArgs);
                return;
            }

            if (_consumerCts.IsCancellationRequested)
            {
                await handleNackForConsumerReceived(eventArgs);
                return;
            }

            // TODO: Track the Message ID as soon as we can identify it
            //       this is so we can intentionally Nack and not Requeue
            //       messages that we've seen redelivered multiple times
            //       (assuming that the Async Method is performing a Nack)

            // NOTE: Also consider the concept of taking longer to Nack - so essentially spinning off some task that will delay X seconds / minutes and then send the Nack.
            //       This will ideally reduce the redelivery rate which will potentially help the Async Method dependents to recovery (e.g. a Database Connection).
            //       After we've continued to receive the same redelivered message X times over X period (or maybe just the period), then we Nack without requeue and send it to the DLX

            try
            {
                await asyncMethod(receivedMessage, _consumerCts.Token);
            }
            catch (AlreadyClosedException)
            {
                await handleNackForConsumerReceived(eventArgs);
            }
            catch (Exception e)
            {
                if(ConsumerException != null)
                {
                    ConsumerException(e);
                }

                await handleNackForConsumerReceived(eventArgs);
            }
        }

        private async Task handleNackForConsumerReceived(BasicDeliverEventArgs eventArgs)
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

                Guid messageId;

                if (eventArgs.BasicProperties == null || eventArgs.BasicProperties.IsMessageIdPresent() == false || Guid.TryParse(eventArgs.BasicProperties.MessageId, out messageId) == false || messageId == Guid.Empty)
                {
                    // Nack without Redelivering as we cannot track how many times this message has been redelivered

                    await TrySendNack(eventArgs.DeliveryTag, false);
                    return;
                }

                RedeliveredMessage message = _consumerRedeliveredMessages.GetValueOrDefault(messageId, new RedeliveredMessage
                {
                    MessageID = messageId,
                    LastTimestamp = DateTime.Now,
                    RedeliveredCount = 1,
                });

                bool redeliver = true;

                if (message.RedeliveredCount >= 3)
                {
                    redeliver = false;
                }

                await TrySendNack(eventArgs.DeliveryTag, redeliver);

                if (redeliver)
                {
                    _consumerRedeliveredMessages.AddOrUpdate(messageId, message, (existingKey, existingMessage) =>
                    {
                        existingMessage.LastTimestamp = DateTime.Now;
                        existingMessage.RedeliveredCount++;

                        return existingMessage;
                    });
                }
                else
                {
                    RedeliveredMessage removedMessage;

                    _consumerRedeliveredMessages.TryRemove(messageId, out removedMessage);
                }
            }
            catch (AlreadyClosedException)
            {
            }
            catch (Exception e)
            {
                if (ConsumerException != null)
                {
                    ConsumerException(e);
                }
            }
        }

        private async Task publishMessagesHandler()
        {
            CancellationToken cancellationToken = _publishMessagesCts.Token;

            try
            {
                while(cancellationToken.IsCancellationRequested == false)
                {
                    foreach(PublishMessage message in _publishMessages.Values.Where(message => DateTime.Now.Subtract(message.PublishTimestamp) >= message.PublishTimeout))
                    {
                        try
                        {
                            if (message.HasRetriesAvailable)
                            {
                                await Publish(message, cancellationToken);
                            }
                            else
                            {
                                transitionPublishMessageToResult(message.MessageID, enPublishResult.Timeout);
                            }
                        }
                        catch (OperationCanceledException)
                        {
                            throw;
                        }
                        catch (Exception e)
                        {
                            if(PublishException != null)
                            {
                                PublishException(e);
                            }
                        }
                    }

                    await Task.Delay(100); // TODO: Get smarter about checking when the next Message might reach its Publish Timeout and/or Get awoken when a new message is published?
                }
            }
            catch (OperationCanceledException)
            {
            }
        }

        private void transitionPublishMessageToResult(Guid messageId, enPublishResult result)
        {
            PublishMessage message;

            if(_publishMessages.TryRemove(messageId, out message))
            {
                try
                {
                    _publishResultsChannel.Writer.TryWrite(new PublishResult
                    {
                        MessageID = messageId,
                        MessageType = message.Type,
                        Result = result,
                    });
                }
                catch (Exception e)
                {
                    if (PublishException != null)
                    {
                        PublishException(e);
                    }
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
                        if (publishResult.MessageType != null && _publishResultsAsyncMethods.TryGetValue(publishResult.MessageType, out Func<Guid, enPublishResult, CancellationToken, Task> asyncMethod))
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

        private async Task publishResultAsync(PublishResult result, Func<Guid, enPublishResult, CancellationToken, Task> asyncMethod, SemaphoreSlim semaphore, CancellationToken cancellationToken)
        {
            try
            {
                Task asyncTask = asyncMethod(result.MessageID, result.Result, cancellationToken);

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
                if(PublishException != null)
                {
                    PublishException(e);
                }
            }
            finally
            {
                semaphore.Release();
            }
        }

        #endregion


        #region Events

        public event ExceptionEventHandler ConnectionException;
        public event ExceptionEventHandler ChannelException;
        public event ExceptionEventHandler ConsumerException;
        public event ExceptionEventHandler PublishException;

        #endregion
    }
}

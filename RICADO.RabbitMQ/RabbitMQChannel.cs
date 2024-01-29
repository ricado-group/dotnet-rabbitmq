using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace RICADO.RabbitMQ
{
    public abstract class RabbitMQChannel : IRabbitMQChannel
    {
        #region Private Fields

        private readonly RabbitMQConnection _connection;
        private readonly SemaphoreSlim _channelSemaphore;

        private IModel _channel;

        private bool _channelShutdown = false;
        private readonly object _channelShutdownLock = new object();

        #endregion


        #region Protected Properties

        protected RabbitMQConnection Connection => _connection;
        
        protected SemaphoreSlim ChannelSemaphore => _channelSemaphore;

        protected IModel Channel => _channel;

        #endregion


        #region Public Properties

        public bool IsConnected => _channel?.IsOpen ?? false;

        public bool IsShutdown
        {
            get
            {
                lock (_channelShutdownLock)
                {
                    return _channelShutdown;
                }
            }
            private set
            {
                lock (_channelShutdownLock)
                {
                    _channelShutdown = value;
                }
            }
        }

        #endregion


        #region Constructor

        internal RabbitMQChannel(RabbitMQConnection connection)
        {
            _connection = connection;
            _channelSemaphore = new SemaphoreSlim(1, 1);
        }

        #endregion


        #region Public Methods

        public virtual async Task Initialize(CancellationToken cancellationToken)
        {
            IsShutdown = false;

            await initializeChannel(cancellationToken);
        }

        public virtual async Task Destroy(CancellationToken cancellationToken)
        {
            IsShutdown = true;

            if (!_channelSemaphore.Wait(0))
            {
                await _channelSemaphore.WaitAsync(cancellationToken);
            }

            try
            {
                destroyChannel();
            }
            finally
            {
                _channelSemaphore.Release();
            }
        }

        public async Task DeclareExchange(string name, ExchangeType type, bool durable, bool autoDelete, CancellationToken cancellationToken)
        {
            if (name == null)
            {
                throw new ArgumentNullException(nameof(name));
            }

            if (IsShutdown == true)
            {
                throw new RabbitMQException("Cannot Declare an Exchange since the Channel is Shutdown");
            }

            if (_connection.IsShutdown == true)
            {
                throw new RabbitMQException("Cannot Declare an Exchange since the Connection is Shutdown");
            }

            if (!_channelSemaphore.Wait(0))
            {
                await _channelSemaphore.WaitAsync(cancellationToken);
            }

            try
            {
                if (IsConnected == false)
                {
                    throw new RabbitMQException("Cannot Declare an Exchange while the Channel is Unavailable");
                }

                if (_connection.IsConnected == false)
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

        public async Task DeclareExchangePassive(string name, CancellationToken cancellationToken)
        {
            if (name == null)
            {
                throw new ArgumentNullException(nameof(name));
            }

            if (IsShutdown == true)
            {
                throw new RabbitMQException("Cannot Passively Declare an Exchange since the Channel is Shutdown");
            }

            if (_connection.IsShutdown == true)
            {
                throw new RabbitMQException("Cannot Passively Declare an Exchange since the Connection is Shutdown");
            }

            if (!_channelSemaphore.Wait(0))
            {
                await _channelSemaphore.WaitAsync(cancellationToken);
            }

            try
            {
                if (IsConnected == false)
                {
                    throw new RabbitMQException("Cannot Passively Declare an Exchange while the Channel is Unavailable");
                }

                if (_connection.IsConnected == false)
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

        public async Task DeleteExchange(string name, bool ifUnused, CancellationToken cancellationToken)
        {
            if (name == null)
            {
                throw new ArgumentNullException(nameof(name));
            }

            if (IsShutdown == true)
            {
                throw new RabbitMQException("Cannot Delete an Exchange since the Channel is Shutdown");
            }

            if (_connection.IsShutdown == true)
            {
                throw new RabbitMQException("Cannot Delete an Exchange since the Connection is Shutdown");
            }

            if (!_channelSemaphore.Wait(0))
            {
                await _channelSemaphore.WaitAsync(cancellationToken);
            }

            try
            {
                if (IsConnected == false)
                {
                    throw new RabbitMQException("Cannot Delete an Exchange while the Channel is Unavailable");
                }

                if (_connection.IsConnected == false)
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

        public async Task DeclareQueue(string name, bool durable, bool exclusive, bool autoDelete, CancellationToken cancellationToken, string deadLetterExchangeName = null)
        {
            if (name == null)
            {
                throw new ArgumentNullException(nameof(name));
            }

            if (IsShutdown == true)
            {
                throw new RabbitMQException("Cannot Declare a Queue since the Channel is Shutdown");
            }

            if (_connection.IsShutdown == true)
            {
                throw new RabbitMQException("Cannot Declare a Queue since the Connection is Shutdown");
            }

            if (!_channelSemaphore.Wait(0))
            {
                await _channelSemaphore.WaitAsync(cancellationToken);
            }

            try
            {
                if (IsConnected == false)
                {
                    throw new RabbitMQException("Cannot Declare a Queue while the Channel is Unavailable");
                }

                if (_connection.IsConnected == false)
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

        public async Task DeclareQueuePassive(string name, CancellationToken cancellationToken)
        {
            if (name == null)
            {
                throw new ArgumentNullException(nameof(name));
            }

            if (IsShutdown == true)
            {
                throw new RabbitMQException("Cannot Passively Declare a Queue since the Channel is Shutdown");
            }

            if (_connection.IsShutdown == true)
            {
                throw new RabbitMQException("Cannot Passively Declare a Queue since the Connection is Shutdown");
            }

            if (!_channelSemaphore.Wait(0))
            {
                await _channelSemaphore.WaitAsync(cancellationToken);
            }

            try
            {
                if (IsConnected == false)
                {
                    throw new RabbitMQException("Cannot Passively Declare a Queue while the Channel is Unavailable");
                }

                if (_connection.IsConnected == false)
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

        public async Task<uint> DeleteQueue(string name, bool ifUnused, bool ifEmpty, CancellationToken cancellationToken)
        {
            if (name == null)
            {
                throw new ArgumentNullException(nameof(name));
            }

            if (IsShutdown == true)
            {
                throw new RabbitMQException("Cannot Delete a Queue since the Channel is Shutdown");
            }

            if (_connection.IsShutdown == true)
            {
                throw new RabbitMQException("Cannot Delete a Queue since the Connection is Shutdown");
            }

            if (!_channelSemaphore.Wait(0))
            {
                await _channelSemaphore.WaitAsync(cancellationToken);
            }

            try
            {
                if (IsConnected == false)
                {
                    throw new RabbitMQException("Cannot Delete a Queue while the Channel is Unavailable");
                }

                if (_connection.IsConnected == false)
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
                throw new RabbitMQException("Cannot Bind an Exchange to another Exchange since the Channel is Shutdown");
            }

            if (_connection.IsShutdown == true)
            {
                throw new RabbitMQException("Cannot Bind an Exchange to another Exchange since the Connection is Shutdown");
            }

            if (!_channelSemaphore.Wait(0))
            {
                await _channelSemaphore.WaitAsync(cancellationToken);
            }

            try
            {
                if (IsConnected == false)
                {
                    throw new RabbitMQException("Cannot Bind an Exchange to another Exchange while the Channel is Unavailable");
                }

                if (_connection.IsConnected == false)
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
                throw new RabbitMQException("Cannot Unbind an Exchange from another Exchange since the Channel is Shutdown");
            }

            if (_connection.IsShutdown == true)
            {
                throw new RabbitMQException("Cannot Unbind an Exchange from another Exchange since the Connection is Shutdown");
            }

            if (!_channelSemaphore.Wait(0))
            {
                await _channelSemaphore.WaitAsync(cancellationToken);
            }

            try
            {
                if (IsConnected == false)
                {
                    throw new RabbitMQException("Cannot Unbind an Exchange from another Exchange while the Channel is Unavailable");
                }

                if (_connection.IsConnected == false)
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
                throw new RabbitMQException("Cannot Bind a Queue to an Exchange since the Channel is Shutdown");
            }

            if (_connection.IsShutdown == true)
            {
                throw new RabbitMQException("Cannot Bind a Queue to an Exchange since the Connection is Shutdown");
            }

            if (!_channelSemaphore.Wait(0))
            {
                await _channelSemaphore.WaitAsync(cancellationToken);
            }

            try
            {
                if (IsConnected == false)
                {
                    throw new RabbitMQException("Cannot Bind a Queue to an Exchange while the Channel is Unavailable");
                }

                if (_connection.IsConnected == false)
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
                throw new RabbitMQException("Cannot Unbind a Queue from an Exchange since the Channel is Shutdown");
            }

            if (_connection.IsShutdown == true)
            {
                throw new RabbitMQException("Cannot Unbind a Queue from an Exchange since the Connection is Shutdown");
            }

            if (!_channelSemaphore.Wait(0))
            {
                await _channelSemaphore.WaitAsync(cancellationToken);
            }

            try
            {
                if (IsConnected == false)
                {
                    throw new RabbitMQException("Cannot Unbind a Queue from an Exchange while the Channel is Unavailable");
                }

                if (_connection.IsConnected == false)
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

        #endregion


        #region Protected Methods

        protected abstract Task InitializeChannel(CancellationToken cancellationToken);

        protected abstract void DestroyChannel();

        #endregion


        #region Private Methods

        private async Task initializeChannel(CancellationToken cancellationToken)
        {
            if (_channel != null)
            {
                throw new RabbitMQException("The Channel was already Initialized");
            }

            cancellationToken.ThrowIfCancellationRequested();

            while (cancellationToken.IsCancellationRequested == false)
            {
                if (_connection == null)
                {
                    throw new RabbitMQException("The Connection was Null during Channel Initialization");
                }

                TimeSpan connectionRetryInterval = _connection.ConnectionRecoveryInterval;

                if (_connection.IsConnected)
                {
                    if (!_channelSemaphore.Wait(0))
                    {
                        await _channelSemaphore.WaitAsync(cancellationToken);
                    }

                    try
                    {
                        _channel = _connection.CreateUnderlyingChannel();

                        if (_channel != null && _channel.IsOpen == true)
                        {
                            _channel.CallbackException += channelCallbackException;
                            _channel.ModelShutdown += channelModelShutdown;

                            await InitializeChannel(cancellationToken);

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
            DestroyChannel();

            if (_channel == null)
            {
                return;
            }

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
            if (e == null || e.Initiator == ShutdownInitiator.Application || e.ReplyText == null)
            {
                return;
            }

            if (e.ReplyCode == 0 || e.ReplyCode == 320)
            {
                return;
            }

            if (_channel.IsOpen == true) // Ignore Shutdown Errors when the Channel is still usable
            {
                return;
            }

            lock (_channelShutdownLock)
            {
                if (_channelShutdown == true)
                {
                    return;
                }

                _channelShutdown = true;
            }

            UnexpectedChannelShutdown?.Invoke(this, e.ReplyCode, e.ReplyText);
        }

        #endregion


        #region Events

        public event ChannelExceptionHandler ChannelException;

        public event UnexpectedChannelShutdownHandler UnexpectedChannelShutdown;

        #endregion
    }
}

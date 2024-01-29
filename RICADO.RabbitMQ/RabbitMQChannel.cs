using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
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

        private readonly ConcurrentDictionary<string, ExchangeDeclaration> _exchangeDeclarations = new ConcurrentDictionary<string, ExchangeDeclaration>();
        private readonly ConcurrentDictionary<string, string> _passiveExchangeDeclarations = new ConcurrentDictionary<string, string>();
        private readonly ConcurrentDictionary<string, QueueDeclaration> _queueDeclarations = new ConcurrentDictionary<string, QueueDeclaration>();
        private readonly ConcurrentDictionary<string, string> _passiveQueueDeclarations = new ConcurrentDictionary<string, string>();
        private readonly ConcurrentDictionary<string, ExchangeBinding> _exchangeBindings = new ConcurrentDictionary<string, ExchangeBinding>();
        private readonly ConcurrentDictionary<string, QueueBinding> _queueBindings = new ConcurrentDictionary<string, QueueBinding>();

        private Channel<bool> _autoRecoveryChannel;
        private CancellationTokenSource _autoRecoveryCts;
        private Task _autoRecoveryTask;

        #endregion


        #region Protected Properties

        protected RabbitMQConnection Connection => _connection;
        
        protected SemaphoreSlim ChannelSemaphore => _channelSemaphore;

        protected IModel Channel => _channel;

        protected IReadOnlyCollection<string> DeclaredQueueNames => new ReadOnlyCollection<string>(_queueDeclarations.Keys.Concat(_passiveQueueDeclarations.Keys).ToList());

        protected Channel<bool> AutoRecoveryChannel => _autoRecoveryChannel;

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
            protected set
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

            _autoRecoveryChannel = System.Threading.Channels.Channel.CreateUnbounded<bool>(new System.Threading.Channels.UnboundedChannelOptions { SingleReader = true, SingleWriter = false, AllowSynchronousContinuations = false });
            _autoRecoveryCts = new CancellationTokenSource();
            _autoRecoveryTask = Task.Run(autoRecoveryHandler, CancellationToken.None);

            await initializeChannel(cancellationToken);
        }

        public virtual async Task Destroy(CancellationToken cancellationToken)
        {
            IsShutdown = true;

            _autoRecoveryChannel?.Writer?.Complete();

            _autoRecoveryCts?.Cancel();

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

            try
            {
                if(_autoRecoveryTask != null)
                {
                    await _autoRecoveryTask;
                }
            }
            catch
            {
            }

            _autoRecoveryChannel = null;
            _autoRecoveryTask = null;

            _exchangeDeclarations.Clear();
            _queueDeclarations.Clear();
            _exchangeBindings.Clear();
            _queueBindings.Clear();
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

                ExchangeDeclaration declaration = new ExchangeDeclaration()
                {
                    Name = name,
                    Type = type,
                    Durable = durable,
                    AutoDelete = autoDelete,
                };

                _exchangeDeclarations.AddOrUpdate(name, declaration, (key, existingDeclaration) => declaration);
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

                _passiveExchangeDeclarations.TryAdd(name, name);
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

                _exchangeDeclarations.TryRemove(name, out _);
                _passiveExchangeDeclarations.TryRemove(name, out _);

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

                QueueDeclaration declaration = new QueueDeclaration()
                {
                    Name = name,
                    Durable = durable,
                    Exclusive = exclusive,
                    AutoDelete = autoDelete,
                    Properties = properties,
                };

                _queueDeclarations.AddOrUpdate(name, declaration, (key, existingDeclaration) => declaration);
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

                _passiveQueueDeclarations.TryAdd(name, name);
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

                _queueDeclarations.TryRemove(name, out _);
                _passiveQueueDeclarations.TryRemove(name, out _);

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

                ExchangeBinding binding = new ExchangeBinding()
                {
                    DestinationName = destinationName,
                    SourceName = sourceName,
                    RoutingKey = routingKey,
                };

                _exchangeBindings.AddOrUpdate(binding.GetUniqueKey(), binding, (key, existingBinding) => binding);
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

                ExchangeBinding binding = new ExchangeBinding()
                {
                    DestinationName = destinationName,
                    SourceName = sourceName,
                    RoutingKey = routingKey,
                };

                _exchangeBindings.TryRemove(binding.GetUniqueKey(), out _);

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

                QueueBinding binding = new QueueBinding()
                {
                    QueueName = queueName,
                    ExchangeName = exchangeName,
                    RoutingKey = routingKey,
                };

                _queueBindings.AddOrUpdate(binding.GetUniqueKey(), binding, (key, existingBinding) => binding);
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

                QueueBinding binding = new QueueBinding()
                {
                    QueueName = queueName,
                    ExchangeName = exchangeName,
                    RoutingKey = routingKey,
                };

                _queueBindings.TryRemove(binding.GetUniqueKey(), out _);

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

        protected abstract Task<bool> TryAutoRecovery(CancellationToken cancellationToken);

        protected void RaiseUnexpectedShutdown(int errorCode, string reason)
        {
            try
            {
                UnexpectedChannelShutdown?.Invoke(this, errorCode, reason);
            }
            catch (Exception ex)
            {
                try
                {
                    ChannelException?.Invoke(this, ex);
                }
                catch
                {
                }
            }
        }

        #endregion


        #region Private Methods

        private Task initializeChannel(CancellationToken cancellationToken)
        {
            if (_channel != null)
            {
                throw new RabbitMQException("The Channel was already Initialized");
            }

            if (_connection == null)
            {
                throw new RabbitMQException("The Connection was Null during Channel Initialization");
            }

            if(_connection.IsConnected == false)
            {
                throw new RabbitMQException("The Connection was not Connected during Channel Initialization");
            }

            _channel = _connection.CreateUnderlyingChannel();

            if (_channel == null || _channel.IsOpen != true)
            {
                throw new RabbitMQException("The Channel failed to Open after being Created");
            }

            _channel.CallbackException += channelCallbackException;
            _channel.ModelShutdown += channelModelShutdown;

            return InitializeChannel(cancellationToken);
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
            if (e == null || e.Exception == null)
            {
                return;
            }

            try
            {
                ChannelException?.Invoke(this, e.Exception);
            }
            catch
            {
            }
        }

        private void channelModelShutdown(object sender, ShutdownEventArgs e)
        {
            if (e == null || e.Initiator == ShutdownInitiator.Application || e.ReplyText == null)
            {
                return;
            }

            if (e.ReplyCode == 0)
            {
                return;
            }

            if(_connection.IsConnected == false || _connection.IsShutdown == true)
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

            try
            {
                UnexpectedChannelShutdown?.Invoke(this, e.ReplyCode, e.ReplyText);
            }
            catch (Exception ex)
            {
                try
                {
                    ChannelException?.Invoke(this, ex);
                }
                catch
                {
                }
            }

            _autoRecoveryChannel?.Writer?.TryWrite(true);
        }

        private async Task autoRecoveryHandler()
        {
            CancellationToken cancellationToken = _autoRecoveryCts.Token;

            try
            {
                while(await _autoRecoveryChannel.Reader.WaitToReadAsync(cancellationToken))
                {
                    while(_autoRecoveryChannel.Reader.TryRead(out bool request))
                    {
                        try
                        {
                            if (await tryAutoRecovery(cancellationToken) == false)
                            {
                                scheduleAutoRecoveryRetry(cancellationToken);
                            }

                            if(cancellationToken.IsCancellationRequested == false)
                            {
                                lock (_channelShutdownLock)
                                {
                                    _channelShutdown = false;
                                }
                            }
                        }
                        catch (OperationCanceledException)
                        {
                            throw;
                        }
                        catch (Exception e)
                        {
                            try
                            {
                                ChannelRecoveryError?.Invoke(this, e);
                            }
                            catch { }

                            scheduleAutoRecoveryRetry(cancellationToken);
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

        private async Task<bool> tryAutoRecovery(CancellationToken cancellationToken)
        {
            await _channelSemaphore.WaitAsync(cancellationToken);

            try
            {
                destroyChannel();

                cancellationToken.ThrowIfCancellationRequested();

                await initializeChannel(cancellationToken);

                foreach (ExchangeDeclaration declaration in _exchangeDeclarations.Values)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    try
                    {
                        _channel.ExchangeDeclare(declaration.Name, declaration.Type.ToString().ToLower(), declaration.Durable, declaration.AutoDelete);
                    }
                    catch (OperationCanceledException)
                    {
                        throw;
                    }
                    catch(Exception e)
                    {
                        throw new RabbitMQException("Failed to Declare Exchange '" + declaration.Name + "' during Auto Recovery - " + e.Message);
                    }
                }

                foreach (string exchangeName in _passiveExchangeDeclarations.Values)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    try
                    {
                        _channel.ExchangeDeclarePassive(exchangeName);
                    }
                    catch (OperationCanceledException)
                    {
                        throw;
                    }
                    catch (Exception e)
                    {
                        throw new RabbitMQException("Failed to Passively Declare Exchange '" + exchangeName + "' during Auto Recovery - " + e.Message);
                    }
                }

                foreach (QueueDeclaration declaration in _queueDeclarations.Values)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    try
                    {
                        QueueDeclareOk result = _channel.QueueDeclare(declaration.Name, declaration.Durable, declaration.Exclusive, declaration.AutoDelete, declaration.Properties);

                        if (result == null || result.QueueName != declaration.Name)
                        {
                            throw new RabbitMQException("The Broker Queue Name did not match the Client Queue Name");
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        throw;
                    }
                    catch (Exception e)
                    {
                        throw new RabbitMQException("Failed to Declare Queue '" + declaration.Name + "' during Auto Recovery - " + e.Message);
                    }
                }

                foreach (string queueName in _passiveQueueDeclarations.Values)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    try
                    {
                        _channel.QueueDeclarePassive(queueName);
                    }
                    catch (OperationCanceledException)
                    {
                        throw;
                    }
                    catch (Exception e)
                    {
                        throw new RabbitMQException("Failed to Passively Declare Queue '" + queueName + "' during Auto Recovery - " + e.Message);
                    }
                }

                foreach (ExchangeBinding binding in _exchangeBindings.Values)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    try
                    {
                        _channel.ExchangeBind(binding.DestinationName, binding.SourceName, binding.RoutingKey);
                    }
                    catch (OperationCanceledException)
                    {
                        throw;
                    }
                    catch (Exception e)
                    {
                        throw new RabbitMQException("Failed to Bind Exchange '" + binding.SourceName + "' to Exchange '" + binding.DestinationName + "' during Auto Recovery - " + e.Message);
                    }
                }

                foreach (QueueBinding binding in _queueBindings.Values)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    try
                    {
                        _channel.QueueBind(binding.QueueName, binding.ExchangeName, binding.RoutingKey);
                    }
                    catch (OperationCanceledException)
                    {
                        throw;
                    }
                    catch (Exception e)
                    {
                        throw new RabbitMQException("Failed to Bind Queue '" + binding.QueueName + "' to Exchange '" + binding.ExchangeName + "' during Auto Recovery - " + e.Message);
                    }
                }

                if(await TryAutoRecovery(cancellationToken) != true)
                {
                    return false;
                }

                try
                {
                    ChannelRecovered?.Invoke(this);
                }
                catch
                {
                }

                return true;
            }
            finally
            {
                _channelSemaphore.Release();
            }
        }

        private void scheduleAutoRecoveryRetry(CancellationToken cancellationToken)
        {
            _ = Task.Delay(_connection.ConnectionRecoveryInterval, cancellationToken).ContinueWith(task => _autoRecoveryChannel?.Writer?.TryWrite(true));
        }

        #endregion


        #region Events

        public event ChannelExceptionHandler ChannelException;

        public event UnexpectedChannelShutdownHandler UnexpectedChannelShutdown;

        public event ChannelRecoveredHandler ChannelRecovered;

        public event ChannelRecoveryErrorHandler ChannelRecoveryError;

        #endregion


        #region Structs

        protected struct ExchangeDeclaration
        {
            public string Name;
            public ExchangeType Type;
            public bool Durable;
            public bool AutoDelete;
        }

        protected struct QueueDeclaration
        {
            public string Name;
            public bool Durable;
            public bool Exclusive;
            public bool AutoDelete;
            public Dictionary<string, object> Properties;
        }

        protected struct ExchangeBinding
        {
            public string SourceName;
            public string DestinationName;
            public string RoutingKey;

            public string GetUniqueKey() => $"{SourceName ?? "NoSource"}::{DestinationName ?? "NoDestination"}::{RoutingKey ?? "NoRoutingKey"}";
        }

        protected struct QueueBinding
        {
            public string QueueName;
            public string ExchangeName;
            public string RoutingKey;

            public string GetUniqueKey() => $"{QueueName ?? "NoQueue"}::{ExchangeName ?? "NoExchange"}::{RoutingKey ?? "NoRoutingKey"}";
        }

        #endregion
    }
}

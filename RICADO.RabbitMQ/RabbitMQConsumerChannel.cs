using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;

namespace RICADO.RabbitMQ
{
    public sealed class RabbitMQConsumerChannel : RabbitMQChannel, IRabbitMQChannel, IRabbitMQConsumerChannel
    {
        #region Private Fields

        private ushort _prefetchCount = 1;

        private AsyncEventingBasicConsumer _consumer;
        private CancellationTokenSource _consumerCts;

        private readonly ConcurrentDictionary<string, ConsumerReceiveHandler> _consumerAsyncMethods = new ConcurrentDictionary<string, ConsumerReceiveHandler>();
        private readonly ConcurrentDictionary<Guid, RedeliveredMessage> _consumerRedeliveredMessages = new ConcurrentDictionary<Guid, RedeliveredMessage>();

        #endregion


        #region Public Properties

        public ushort PrefetchCount
        {
            get
            {
                return _prefetchCount;
            }
            set
            {
                _prefetchCount = value;
            }
        }

        #endregion


        #region Constructor

        internal RabbitMQConsumerChannel(RabbitMQConnection connection) : base(connection)
        {
        }

        #endregion


        #region Public Methods

        public override async Task Destroy(CancellationToken cancellationToken)
        {
            await base.Destroy(cancellationToken);

            _consumerAsyncMethods.Clear();

            _consumerRedeliveredMessages.Clear();
        }

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
                throw new RabbitMQException("Cannot Create a Queue Consumer since the Channel is Shutdown");
            }

            if (Connection.IsShutdown == true)
            {
                throw new RabbitMQException("Cannot Create a Queue Consumer since the Connection is Shutdown");
            }

            if (!ChannelSemaphore.Wait(0))
            {
                await ChannelSemaphore.WaitAsync(cancellationToken);
            }

            try
            {
                if (IsConnected == false)
                {
                    throw new RabbitMQException("Cannot Create a Queue Consumer while the Channel is Unavailable");
                }

                if (Connection.IsConnected == false)
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

                if (Channel.BasicConsume(queueName, false, queueName, _consumer) != queueName)
                {
                    _consumerAsyncMethods.TryRemove(queueName, out _);

                    throw new RabbitMQException("Failed to Create the Queue Consumer - The Broker Consumer Tag did not match the Client Consumer Tag");
                }
            }
            finally
            {
                ChannelSemaphore.Release();
            }
        }

        public async Task DestroyQueueConsumer(string queueName, CancellationToken cancellationToken)
        {
            if (queueName == null)
            {
                throw new ArgumentNullException(nameof(queueName));
            }

            if (IsShutdown == true)
            {
                throw new RabbitMQException("Cannot Destroy a Queue Consumer since the Channel is Shutdown");
            }

            if (Connection.IsShutdown == true)
            {
                throw new RabbitMQException("Cannot Destroy a Queue Consumer since the Connection is Shutdown");
            }

            if (!ChannelSemaphore.Wait(0))
            {
                await ChannelSemaphore.WaitAsync(cancellationToken);
            }

            try
            {
                if (IsConnected == false)
                {
                    throw new RabbitMQException("Cannot Destroy a Queue Consumer while the Channel is Unavailable");
                }

                if (Connection.IsConnected == false)
                {
                    throw new RabbitMQException("Cannot Destroy a Queue Consumer while the Connection is Unavailable");
                }

                if (_consumer == null)
                {
                    throw new RabbitMQException("Cannot Destroy a Queue Consumer since the Consumer Handler is Null");
                }

                _consumerAsyncMethods.TryRemove(queueName, out _);

                Channel.BasicCancel(queueName);
            }
            finally
            {
                ChannelSemaphore.Release();
            }
        }

        #endregion


        #region Internal Methods

#if NETSTANDARD
        /// <summary>
        /// Send an Ack Response for one or more Delivery Tags to the RabbitMQ Broker
        /// </summary>
        /// <param name="deliveryTag">The Delivery Tag to Ack</param>
        /// <param name="cancellationToken">A Cancellation Token that can be used to signal the Asynchronous Operation should be Cancelled</param>
        /// <param name="multiple">Whether all Delivery Tags prior to and including this Tag should be Ack'd</param>
        /// <returns>A Task that will Complete upon successfully Sending an Ack Response to the RabbitMQ Broker</returns>
        internal async Task SendAck(ulong deliveryTag, CancellationToken cancellationToken, bool multiple = false)
        {
            if (IsShutdown == true)
            {
                throw new RabbitMQException("Cannot Ack a Message since the Channel is Shutdown");
            }

            if (Connection.IsShutdown == true)
            {
                throw new RabbitMQException("Cannot Ack a Message since the Connection is Shutdown");
            }

            if (IsConnected == false)
            {
                throw new RabbitMQException("Cannot Ack a Message while the Channel is Unavailable");
            }

            if (Connection.IsConnected == false)
            {
                throw new RabbitMQException("Cannot Ack a Message while the Connection is Unavailable");
            }

            if (!ChannelSemaphore.Wait(0))
            {
                await ChannelSemaphore.WaitAsync(cancellationToken);
            }

            try
            {
                Channel.BasicAck(deliveryTag, multiple);
            }
            catch (AlreadyClosedException e)
            {
                throw new RabbitMQException("Failed to Ack a Message - The Connection or Channel is Closed", e);
            }
            finally
            {
                ChannelSemaphore.Release();
            }
        }
#else
        /// <summary>
        /// Send an Ack Response for one or more Delivery Tags to the RabbitMQ Broker
        /// </summary>
        /// <param name="deliveryTag">The Delivery Tag to Ack</param>
        /// <param name="cancellationToken">A Cancellation Token that can be used to signal the Asynchronous Operation should be Cancelled</param>
        /// <param name="multiple">Whether all Delivery Tags prior to and including this Tag should be Ack'd</param>
        /// <returns>A Task that will Complete upon successfully Sending an Ack Response to the RabbitMQ Broker</returns>
        internal async ValueTask SendAck(ulong deliveryTag, CancellationToken cancellationToken, bool multiple = false)
        {
            if (IsShutdown == true)
            {
                throw new RabbitMQException("Cannot Ack a Message since the Channel is Shutdown");
            }

            if (Connection.IsShutdown == true)
            {
                throw new RabbitMQException("Cannot Ack a Message since the Connection is Shutdown");
            }

            if (IsConnected == false)
            {
                throw new RabbitMQException("Cannot Ack a Message while the Channel is Unavailable");
            }

            if (Connection.IsConnected == false)
            {
                throw new RabbitMQException("Cannot Ack a Message while the Connection is Unavailable");
            }

            if (!ChannelSemaphore.Wait(0))
            {
                await ChannelSemaphore.WaitAsync(cancellationToken);
            }

            try
            {
                Channel.BasicAck(deliveryTag, multiple);
            }
            catch (AlreadyClosedException e)
            {
                throw new RabbitMQException("Failed to Ack a Message - The Connection or Channel is Closed", e);
            }
            finally
            {
                ChannelSemaphore.Release();
            }
        }
#endif

        /// <summary>
        /// Try to Send an Ack Response for one or more Delivery Tags to the RabbitMQ Broker
        /// </summary>
        /// <param name="deliveryTag">The Delivery Tag to Ack</param>
        /// <param name="cancellationToken">A Cancellation Token that can be used to signal the Asynchronous Operation should be Cancelled</param>
        /// <param name="multiple">Whether all Delivery Tags prior to and including this Tag should be Ack'd</param>
        /// <returns>A Task that will Complete with a Result to indicate if the Ack Response was Sent to the RabbitMQ Broker</returns>
        internal async ValueTask<bool> TrySendAck(ulong deliveryTag, CancellationToken cancellationToken, bool multiple = false)
        {
            if (IsShutdown == true || Connection.IsShutdown == true || IsConnected == false || Connection.IsConnected == false)
            {
                return false;
            }

            try
            {
                await SendAck(deliveryTag, cancellationToken, multiple);

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
        /// <param name="cancellationToken">A Cancellation Token that can be used to signal the Asynchronous Operation should be Cancelled</param>
        /// <param name="multiple">Whether all Delivery Tags prior to and including this Tag should be Nack'd</param>
        /// <returns>A Task that will Complete upon successfully Sending a Nack Response to the RabbitMQ Broker</returns>
        internal async Task SendNack(ulong deliveryTag, bool requeue, CancellationToken cancellationToken, bool multiple = false)
        {
            if (IsShutdown == true)
            {
                throw new RabbitMQException("Cannot Nack a Message since the Channel is Shutdown");
            }

            if (Connection.IsShutdown == true)
            {
                throw new RabbitMQException("Cannot Nack a Message since the Connection is Shutdown");
            }

            if (IsConnected == false)
            {
                throw new RabbitMQException("Cannot Nack a Message while the Channel is Unavailable");
            }

            if (Connection.IsConnected == false)
            {
                throw new RabbitMQException("Cannot Nack a Message while the Connection is Unavailable");
            }

            if (!ChannelSemaphore.Wait(0))
            {
                await ChannelSemaphore.WaitAsync(cancellationToken);
            }

            try
            {
                Channel.BasicNack(deliveryTag, multiple, requeue);
            }
            catch (AlreadyClosedException e)
            {
                throw new RabbitMQException("Failed to Nack a Message - The Connection or Channel is Closed", e);
            }
            finally
            {
                ChannelSemaphore.Release();
            }
        }
#else
        /// <summary>
        /// Send a Nack Response for one or more Delivery Tags to the RabbitMQ Broker
        /// </summary>
        /// <param name="deliveryTag">The Delivery Tag to Nack</param>
        /// <param name="requeue">Whether the Delivery Tag(s) should be Requeued for Delivery by the RabbitMQ Broker</param>
        /// <param name="cancellationToken">A Cancellation Token that can be used to signal the Asynchronous Operation should be Cancelled</param>
        /// <param name="multiple">Whether all Delivery Tags prior to and including this Tag should be Nack'd</param>
        /// <returns>A Task that will Complete upon successfully Sending a Nack Response to the RabbitMQ Broker</returns>
        internal async ValueTask SendNack(ulong deliveryTag, bool requeue, CancellationToken cancellationToken, bool multiple = false)
        {
            if (IsShutdown == true)
            {
                throw new RabbitMQException("Cannot Nack a Message since the Channel is Shutdown");
            }

            if (Connection.IsShutdown == true)
            {
                throw new RabbitMQException("Cannot Nack a Message since the Connection is Shutdown");
            }

            if (IsConnected == false)
            {
                throw new RabbitMQException("Cannot Nack a Message while the Channel is Unavailable");
            }

            if (Connection.IsConnected == false)
            {
                throw new RabbitMQException("Cannot Nack a Message while the Connection is Unavailable");
            }

            if (!ChannelSemaphore.Wait(0))
            {
                await ChannelSemaphore.WaitAsync(cancellationToken);
            }

            try
            {
                Channel.BasicNack(deliveryTag, multiple, requeue);
            }
            catch (AlreadyClosedException e)
            {
                throw new RabbitMQException("Failed to Nack a Message - The Connection or Channel is Closed", e);
            }
            finally
            {
                ChannelSemaphore.Release();
            }
        }
#endif

        /// <summary>
        /// Try to Send a Nack Response for one or more Delivery Tags to the RabbitMQ Broker
        /// </summary>
        /// <param name="deliveryTag">The Delivery Tag to Nack</param>
        /// <param name="requeue">Whether the Delivery Tag(s) should be Requeued for Delivery by the RabbitMQ Broker</param>
        /// <param name="cancellationToken">A Cancellation Token that can be used to signal the Asynchronous Operation should be Cancelled</param>
        /// <param name="multiple">Whether all Delivery Tags prior to and including this Tag should be Nack'd</param>
        /// <returns>A Task that will Complete with a Result to indicate if the Nack Response was Sent to the RabbitMQ Broker</returns>
        internal async ValueTask<bool> TrySendNack(ulong deliveryTag, bool requeue, CancellationToken cancellationToken, bool multiple = false)
        {
            if (IsShutdown == true || Connection.IsShutdown == true || IsConnected == false || Connection.IsConnected == false)
            {
                return false;
            }

            try
            {
                await SendNack(deliveryTag, requeue, cancellationToken, multiple);

                return true;
            }
            catch
            {
                return false;
            }
        }

        #endregion


        #region Protected Methods

        protected override Task InitializeChannel(CancellationToken cancellationToken)
        {
            Channel.BasicQos(0, PrefetchCount, true);

            _consumerCts = new CancellationTokenSource();

            _consumer = new AsyncEventingBasicConsumer(Channel);

            _consumer.Received += consumerReceived;
            _consumer.ConsumerCancelled += consumerCancelled;

            return Task.CompletedTask;
        }

        protected override void DestroyChannel()
        {
            _consumerCts?.Cancel();

            if (_consumer != null)
            {
                _consumer.Received -= consumerReceived;
                _consumer.ConsumerCancelled -= consumerCancelled;
                _consumer = null;
            }
        }

        protected override Task<bool> TryAutoRecovery(CancellationToken cancellationToken)
        {
            _consumerRedeliveredMessages.Clear();

            foreach (string queueName in _consumerAsyncMethods.Keys)
            {
                cancellationToken.ThrowIfCancellationRequested();

                try
                {
                    if (Channel.BasicConsume(queueName, false, queueName, _consumer) != queueName)
                    {
                        throw new RabbitMQException("The Broker Consumer Tag did not match the Client Consumer Tag");
                    }
                }
                catch (OperationCanceledException)
                {
                    throw;
                }
                catch (Exception e)
                {
                    throw new RabbitMQException("Failed to Create a Consumer on Queue '" + queueName + "' during Auto Recovery - " + e.Message);
                }
            }

            return Task.FromResult(true);
        }

        #endregion


        #region Private Methods

        private async Task consumerReceived(object sender, BasicDeliverEventArgs eventArgs)
        {
            if (eventArgs == null)
            {
                return;
            }

            Guid? messageId = extractMessageId(eventArgs);

            if (messageId.HasValue)
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
                    if(await TrySendAck(eventArgs.DeliveryTag, _consumerCts.Token) && messageId.HasValue)
                    {
                        // Remove the Tracked Message since we sent an Ack
                        _consumerRedeliveredMessages.TryRemove(messageId.Value, out _);
                    }
                }
                else if (result == ConsumerResultType.Discard)
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

        private Task consumerCancelled(object sender, ConsumerEventArgs eventArgs)
        {
            if(eventArgs == null || eventArgs.ConsumerTags == null || eventArgs.ConsumerTags.Length == 0)
            {
                return Task.CompletedTask;
            }

            if(Connection.IsConnected == false || Connection.IsShutdown == true)
            {
                return Task.CompletedTask;
            }

            if(eventArgs.ConsumerTags.All(tag => _consumerAsyncMethods.ContainsKey(tag) == false))
            {
                return Task.CompletedTask;
            }

            IEnumerable<string> queueNames = eventArgs.ConsumerTags.Where(tag => _consumerAsyncMethods.ContainsKey(tag));

            if(queueNames.All(queueName => DeclaredQueueNames.Contains(queueName) == false))
            {
                return Task.CompletedTask;
            }

            if(IsShutdown == true)
            {
                return Task.CompletedTask;
            }

            IsShutdown = true;

            RaiseUnexpectedShutdown(404, "Consumer was Unexpectedly Cancelled");

            AutoRecoveryChannel?.Writer?.TryWrite(true);

            return Task.CompletedTask;
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
                    await TrySendNack(eventArgs.DeliveryTag, true, _consumerCts.Token);
                    return;
                }

                if (messageId.HasValue == false || messageId.Value == Guid.Empty)
                {
                    // Nack without Redelivering as we cannot track how many times this message has been redelivered
                    await TrySendNack(eventArgs.DeliveryTag, false, _consumerCts.Token);
                    return;
                }

                bool redeliver = !forceNoRequeue;
                TimeSpan nackDelay = TimeSpan.Zero;

                if (forceNoRequeue == false && _consumerRedeliveredMessages.TryGetValue(messageId.Value, out RedeliveredMessage message))
                {
                    if (message.LastTimestamp.Subtract(message.FirstTimestamp).TotalHours >= 3 && message.RedeliveredCount >= 25)
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

                if (redeliver == true && nackDelay > TimeSpan.Zero)
                {
                    _ = Task.Run(async () =>
                    {
                        await Task.Delay(nackDelay);

                        await TrySendNack(eventArgs.DeliveryTag, true, _consumerCts.Token);
                    });
                }
                else
                {
                    await TrySendNack(eventArgs.DeliveryTag, redeliver, _consumerCts.Token);
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

            if (eventArgs.Redelivered == false)
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

        public event ConsumerExceptionHandler ConsumerException;

        #endregion
    }
}

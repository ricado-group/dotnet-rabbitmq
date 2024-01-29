using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Channels;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;

namespace RICADO.RabbitMQ
{
    public sealed class RabbitMQPublisherChannel : RabbitMQChannel, IRabbitMQChannel, IRabbitMQPublisherChannel
    {
        #region Private Fields

        private readonly ConcurrentDictionary<Guid, PublishMessage> _publishMessages = new ConcurrentDictionary<Guid, PublishMessage>();
        private Task _publishMessagesTask;
        private CancellationTokenSource _publishMessagesCts;

        private readonly ConcurrentDictionary<string, PublishResultHandler> _publishResultsAsyncMethods = new ConcurrentDictionary<string, PublishResultHandler>();
        private Channel<PublishResult> _publishResultsChannel;
        private SemaphoreSlim _publishResultsSemaphore;
        private CancellationTokenSource _publishResultsCts;
        private Task _publishResultsTask;

        private int _unconfirmedMessagesLimit = 10;
        private int _publishResultsConcurrency = 1;
        private TimeSpan _defaultPublishTimeout;
        private int _defaultPublishRetries = 3;

        #endregion


        #region Public Properties

        public int UnconfirmedMessagesLimit
        {
            get
            {
                return _unconfirmedMessagesLimit;
            }
            set
            {
                _unconfirmedMessagesLimit = value;
            }
        }
        
        public int PublishResultsConcurrency
        {
            get
            {
                return _publishResultsConcurrency;
            }
            set
            {
                _publishResultsConcurrency = value;
            }
        }

        public TimeSpan DefaultPublishTimeout
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

        #endregion


        #region Constructor

        internal RabbitMQPublisherChannel(RabbitMQConnection connection, TimeSpan defaultPublishTimeout) : base(connection)
        {
            _defaultPublishTimeout = defaultPublishTimeout;
        }

        #endregion


        #region Public Methods

        public override async Task Initialize(CancellationToken cancellationToken)
        {
            await base.Initialize(cancellationToken);

            _publishResultsChannel = System.Threading.Channels.Channel.CreateUnbounded<PublishResult>(new System.Threading.Channels.UnboundedChannelOptions { SingleReader = true, SingleWriter = false, AllowSynchronousContinuations = false });
            _publishResultsSemaphore = new SemaphoreSlim(_publishResultsConcurrency, _publishResultsConcurrency);
            _publishResultsCts = new CancellationTokenSource();
            _publishResultsTask = Task.Run(publishResultsHandler, CancellationToken.None);

            _publishMessagesCts = new CancellationTokenSource();
            _publishMessagesTask = Task.Run(publishMessagesHandler, CancellationToken.None);
        }

        public override async Task Destroy(CancellationToken cancellationToken)
        {
            _publishResultsChannel?.Writer?.Complete();

            _publishResultsCts?.Cancel();

            _publishMessagesCts?.Cancel();

            await base.Destroy(cancellationToken);

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

        public void UnregisterPublishResultHandler(string messageType)
        {
            if (messageType == null)
            {
                throw new ArgumentNullException(nameof(messageType));
            }

            _publishResultsAsyncMethods.TryRemove(messageType, out _);
        }

        public async ValueTask Publish(PublishMessage message, CancellationToken cancellationToken)
        {
            if (IsShutdown == true)
            {
                throw new RabbitMQException("Cannot Publish a Message since the Channel is Shutdown");
            }

            if (Connection.IsShutdown == true)
            {
                throw new RabbitMQException("Cannot Publish a Message since the Connection is Shutdown");
            }

            if (IsConnected == false)
            {
                throw new RabbitMQException("Cannot Publish a Message while the Channel is Unavailable");
            }

            if (Connection.IsConnected == false)
            {
                throw new RabbitMQException("Cannot Publish a Message while the Connection is Unavailable");
            }

            if(_unconfirmedMessagesLimit > 0 && _publishMessages.Count > _unconfirmedMessagesLimit)
            {
                throw new RabbitMQException("Failed to Publish a Message - The Unconfirmed Messages Limit has been Exceeded");
            }

            if (!ChannelSemaphore.Wait(0, cancellationToken))
            {
                await ChannelSemaphore.WaitAsync(cancellationToken);
            }

            try
            {
                message.Publish(Channel, _publishMessages);
            }
            catch (AlreadyClosedException e)
            {
                _publishMessages.TryRemove(message.MessageID, out _);

                throw new RabbitMQException("Failed to Publish a Message - The Connection or Channel is Closed", e);
            }
            catch (Exception)
            {
                _publishMessages.TryRemove(message.MessageID, out _);

                throw;
            }
            finally
            {
                ChannelSemaphore.Release();
            }
        }

        public async ValueTask<bool> TryPublish(PublishMessage message, CancellationToken cancellationToken)
        {
            if (IsShutdown == true || Connection.IsShutdown == true || IsConnected == false || Connection.IsConnected == false)
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


        #region Protected Methods

        protected override Task InitializeChannel(CancellationToken cancellationToken)
        {
            Channel.BasicAcks += channelReceivedAck;
            Channel.BasicNacks += channelReceivedNack;
            Channel.BasicReturn += channelReceivedReturn;

            Channel.ConfirmSelect();

            return Task.CompletedTask;
        }

        protected override void DestroyChannel()
        {
            Channel.BasicAcks -= channelReceivedAck;
            Channel.BasicNacks -= channelReceivedNack;
            Channel.BasicReturn -= channelReceivedReturn;
        }

        #endregion


        #region Private Methods

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

            transitionPublishMessageToResult(messageId, PublishResultType.Returned, e.ReplyCode, e.ReplyText);
        }

        private async Task publishMessagesHandler()
        {
            CancellationToken cancellationToken = _publishMessagesCts.Token;

            try
            {
                while (cancellationToken.IsCancellationRequested == false)
                {
                    foreach (PublishMessage message in _publishMessages.Values.Where(message => DateTime.UtcNow.Subtract(message.PublishTimestamp) >= message.PublishTimeout))
                    {
                        try
                        {
                            // NOTE: Disabled Retry Publishing for now
                            if (false && message.HasRetriesAvailable)
                            {
                                if (await TryPublish(message, cancellationToken) == false)
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

                    await Task.Delay(500);
                }
            }
            catch (OperationCanceledException)
            {
            }
        }

        private void transitionPublishMessageToResult(Guid messageId, PublishResultType result, int? failureCode = null, string failureReason = null)
        {
            if (_publishMessages.TryRemove(messageId, out PublishMessage message))
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
                while (await _publishResultsChannel.Reader.WaitToReadAsync(cancellationToken))
                {
                    while (_publishResultsChannel.Reader.TryRead(out PublishResult publishResult))
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

                if (asyncTask.IsCompleted == false)
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

        #endregion


        #region Events

        public event PublishExceptionHandler PublishException;

        #endregion
    }
}

using System;
using System.Threading;
using System.Threading.Tasks;

namespace RICADO.RabbitMQ
{
    public delegate Task<bool> ConsumerReceiveHandler(ReceivedMessage message, CancellationToken cancellationToken);
    
    public delegate Task PublishResultHandler(Guid messageId, enPublishResult result, int? failureCode, string failureReason, CancellationToken cancellationToken);
    
    public delegate void ExceptionEventHandler(RabbitMQClient client, Exception e);

    public delegate void ConnectionRecoverySuccessHandler(RabbitMQClient client);

    public delegate void ConnectionRecoveryErrorHandler(RabbitMQClient client, Exception e);

    public delegate void UnexpectedConnectionShutdownHandler(RabbitMQClient client, int errorCode, string reason);
}

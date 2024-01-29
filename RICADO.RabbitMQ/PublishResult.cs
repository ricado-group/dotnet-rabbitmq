using System;

namespace RICADO.RabbitMQ
{
#if NETSTANDARD
    internal struct PublishResult
    {
        public Guid MessageID;
        public string MessageType;
        public PublishResultType Result;
        public int? FailureCode;
        public string FailureReason;
    }
#else
    internal record PublishResult
    {
        public Guid MessageID;
        public string MessageType;
        public PublishResultType Result;
        public int? FailureCode;
        public string FailureReason;
    }
#endif
}

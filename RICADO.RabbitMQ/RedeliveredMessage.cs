using System;

namespace RICADO.RabbitMQ
{
#if NETSTANDARD
    internal struct RedeliveredMessage
    {
        public Guid MessageID;
        public DateTime FirstTimestamp;
        public DateTime LastTimestamp;
        public int RedeliveredCount;
    }
#else
    internal struct RedeliveredMessage
    {
        public Guid MessageID;
        public DateTime FirstTimestamp;
        public DateTime LastTimestamp;
        public int RedeliveredCount;
    }
#endif
}

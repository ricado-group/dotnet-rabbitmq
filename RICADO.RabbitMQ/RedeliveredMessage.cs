using System;

namespace RICADO.RabbitMQ
{
    internal struct RedeliveredMessage
    {
        public Guid MessageID;
        public DateTime LastTimestamp;
        public int RedeliveredCount;
    }
}

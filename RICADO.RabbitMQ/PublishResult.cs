﻿using System;

namespace RICADO.RabbitMQ
{
    internal struct PublishResult
    {
        public Guid MessageID;
        public string MessageType;
        public enPublishResult Result;
    }
}
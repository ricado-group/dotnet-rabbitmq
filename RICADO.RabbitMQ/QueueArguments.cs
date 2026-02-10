using System;
using System.Collections.Generic;

namespace RICADO.RabbitMQ
{
    public abstract record QueueArguments
    {
        /// <summary>
        /// Sets "x-message-ttl" - How long a message published to a queue can live before it is discarded (milliseconds)
        /// </summary>
        public int? MessageTtl { get; set; } // x-message-ttl

        /// <summary>
        /// Sets "x-expires" - How long a queue can be unused for before it is automatically deleted (milliseconds)
        /// </summary>
        public int? QueueTtl { get; set; } // x-expires

        /// <summary>
        /// Sets "x-max-length" - How many (ready) messages a queue can contain before it starts to drop them from its head
        /// </summary>
        public int? MaxMessages { get; set; } // x-max-length

        /// <summary>
        /// Sets "x-max-length-bytes" - Total body size for ready messages a queue can contain before it starts to drop them from its head
        /// </summary>
        public int? MaxTotalBytes { get; set; } // x-max-length-bytes

        /// <summary>
        /// Sets "x-single-active-consumer" - If set, makes sure only one consumer at a time consumes from the queue and fails over to another registered consumer in case the active one is cancelled or dies
        /// </summary>
        public bool? SingleActiveConsumer { get; set; } // x-single-active-consumer

        /// <summary>
        /// Sets "x-dead-letter-exchange" - Optional name of an exchange to which messages will be republished if they are rejected or expire
        /// </summary>
        public string DeadLetterExchange { get; set; } // x-dead-letter-exchange

        /// <summary>
        /// Sets "x-dead-letter-routing-key" - Optional replacement routing key to use when a message is dead-lettered. If this is not set, the message's original routing key will be used
        /// </summary>
        public string DeadLetterRoutingKey { get; set; } // x-dead-letter-routing-key

        internal Dictionary<string, object> ToDictionary()
        {
            Dictionary<string, object> arguments = new Dictionary<string, object>();

            if(MessageTtl.HasValue)
            {
                arguments.Add("x-message-ttl", MessageTtl.Value);
            }

            if (QueueTtl.HasValue)
            {
                arguments.Add("x-expires", QueueTtl.Value);
            }

            if (MaxMessages.HasValue)
            {
                arguments.Add("x-max-length", MaxMessages.Value);
            }

            if (MaxTotalBytes.HasValue)
            {
                arguments.Add("x-max-length-bytes", MaxTotalBytes.Value);
            }

            if (SingleActiveConsumer.HasValue)
            {
                arguments.Add("x-single-active-consumer", SingleActiveConsumer.Value);
            }

            if (DeadLetterExchange != null && DeadLetterExchange.Length > 0)
            {
                arguments.Add("x-dead-letter-exchange", DeadLetterExchange);
            }

            if (DeadLetterRoutingKey != null && DeadLetterRoutingKey.Length > 0)
            {
                arguments.Add("x-dead-letter-routing-key", DeadLetterRoutingKey);
            }

            AddTypeSpecificArguments(arguments);

            return arguments;
        }

        protected abstract void AddTypeSpecificArguments(Dictionary<string, object> arguments);
    }

    public sealed record ClassicQueueArguments : QueueArguments
    {
        public enum OverflowBehaviours
        {
            DropHead, // drop-head
            RejectPublish, // reject-publish
            RejectPublishDlx, // reject-publish-dlx
        }

        /// <summary>
        /// Sets "x-overflow" - This determines what happens to messages when the maximum length of a queue is reached
        /// </summary>
        public OverflowBehaviours? OverflowBehaviour { get; set; } // x-overflow

        /// <summary>
        /// Sets "x-max-priority" - Defines the maximum priority level that a queue will support. Ensures that messages with a higher priority value are delivered to consumers before those with a lower priority
        /// </summary>
        public byte? MaxPriority { get; set; } // x-max-priority

        /// <summary>
        /// Sets "x-queue-version" - Set the queue version (defaults to version 1). Version 1 has a journal-based index that embeds small messages. Version 2 has a different index which improves memory usage and performance in many scenarios, as well as a per-queue store for messages that were previously embedded
        /// </summary>
        public int? QueueVersion { get; set; } // x-queue-version

        protected override void AddTypeSpecificArguments(Dictionary<string, object> arguments)
        {
            if(OverflowBehaviour.HasValue)
            {
                switch (OverflowBehaviour.Value)
                {
                    case OverflowBehaviours.DropHead:
                        arguments.Add("x-overflow", "drop-head");
                        break;

                    case OverflowBehaviours.RejectPublish:
                        arguments.Add("x-overflow", "reject-publish");
                        break;

                    case OverflowBehaviours.RejectPublishDlx:
                        arguments.Add("x-overflow", "reject-publish-dlx");
                        break;
                }
            }

            if(MaxPriority.HasValue)
            {
                arguments.Add("x-max-priority", MaxPriority.Value);
            }

            if(QueueVersion.HasValue)
            {
                arguments.Add("x-queue-version", QueueVersion.Value);
            }
        }
    }

    public sealed record QuorumQueueArguments : QueueArguments
    {
        public enum OverflowBehaviours
        {
            DropHead, // drop-head
            RejectPublish, // reject-publish
        }

        public enum LeaderLocators
        {
            ClientLocal, // client-local
            Balanced, // balanced
        }

        public enum DeadLetterStrategies
        {
            AtLeastOnce, // at-least-once
            AtMostOnce, // at-most-once
        }

        /// <summary>
        /// Sets "x-overflow" - This determines what happens to messages when the maximum length of a queue is reached
        /// </summary>
        public OverflowBehaviours? OverflowBehaviour { get; set; } // x-overflow

        /// <summary>
        /// Sets "x-queue-leader-locator" - Set the rule by which the queue leader is located when declared on a cluster of nodes
        /// </summary>
        public LeaderLocators? LeaderLocator { get; set; } // x-queue-leader-locator

        /// <summary>
        /// Sets "x-quorum-initial-group-size" - Controls the initial replication factor (number of replicas or members) for a quorum queue
        /// </summary>
        public int? QuorumInitialGroupSize { get; set; } // x-quorum-initial-group-size

        /// <summary>
        /// Sets "x-delivery-limit" - The number of allowed unsuccessful delivery attempts. Once a message has been delivered unsuccessfully more than this many times it will be dropped or dead-lettered, depending on the queue configuration
        /// </summary>
        public int? DeliveryLimit { get; set; } // x-delivery-limit

        /// <summary>
        /// Sets "x-dead-letter-strategy" - Defines how quorum queues handle message dead-lettering, specifically choosing between at-least-once (ensures no loss but potential duplicates) and at-most-once (the default - faster, but risky)
        /// </summary>
        public DeadLetterStrategies? DeadLetterStrategy { get; set; } // x-dead-letter-strategy

        protected override void AddTypeSpecificArguments(Dictionary<string, object> arguments)
        {
            if (OverflowBehaviour.HasValue)
            {
                switch (OverflowBehaviour.Value)
                {
                    case OverflowBehaviours.DropHead:
                        arguments.Add("x-overflow", "drop-head");
                        break;

                    case OverflowBehaviours.RejectPublish:
                        arguments.Add("x-overflow", "reject-publish");
                        break;
                }
            }

            if (LeaderLocator.HasValue)
            {
                switch (LeaderLocator.Value)
                {
                    case LeaderLocators.ClientLocal:
                        arguments.Add("x-queue-leader-locator", "client-local");
                        break;

                    case LeaderLocators.Balanced:
                        arguments.Add("x-queue-leader-locator", "balanced");
                        break;
                }
            }

            if (QuorumInitialGroupSize.HasValue)
            {
                arguments.Add("x-quorum-initial-group-size", QuorumInitialGroupSize.Value);
            }

            if (DeliveryLimit.HasValue)
            {
                arguments.Add("x-delivery-limit", DeliveryLimit.Value);
            }

            if (DeadLetterStrategy.HasValue)
            {
                switch (DeadLetterStrategy.Value)
                {
                    case DeadLetterStrategies.AtLeastOnce:
                        arguments.Add("x-dead-letter-strategy", "at-least-once");
                        break;

                    case DeadLetterStrategies.AtMostOnce:
                        arguments.Add("x-dead-letter-strategy", "at-most-once");
                        break;
                }
            }
        }
    }
}

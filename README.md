# Work Queue

This module queues messages (work) sent to it, and then forwards the work to one of many processors that may be attached to it, if available.

Once a processor has processed the work, it replies to the message and when the work queue receives the reply it removes the work from the queue. The reply is then forwarded back to the original sender. The processor can time out in processing a message, in which case the message becomes available again for other processors to consume.

The sender can also receive an optional reply when the work has been accepted by the work queue.

Multiple processors can register for work with the work queue.

The work queue is useful for use cases where you have a lot of work to process and want to share this work out amongst many processors.

One example would be processing an order queue - each order must only be processed once, but we can have many processors (potentially on different machines) processing each order.

Another example would be in computational intensive tasks where the task can be split into N pieces and processed in parallel by different nodes. In other words, a compute cluster.

This module should not be run as a worker.

## Dependencies

If this queue is persistent, the module requires a MongoDB persistor module to be running for persisting the queue data. [WIP]

## Name

The module name is `work-queue`.

## Configuration

This module requires the following configuration:

    {
        "address": <address>,
        "process_timeout": <process_timeout>,
        "persistor_address": <persistor_address>,
        "collection": <collection>   
    }
    
Where:    

* `address` The main address for the module. Every module has a main address.
* `process_timeout` The processing timeout, in milliseconds for each item of work. If work is not processed before the timeout, it is returned to the queue and made available to other processors. This field is optional. Default value is `300000` (5 minutes).
* `persistor_address` If specified, this queue is persistent and this is the address of the persistor module to use for persistence. This field is optional.
* `collection` If persistent, the collection in the persistor where to persist the queue. This field is optional.

An example, non persistent configuration would be:

    {
        "address": "test.orderQueue"
    }
    
An example, persistent configuration would be:

    {
        "address": "test.orderQueue",
        "persistor_address": "test.myPersistor",
        "collection": "order_queue"
    }    

## Operations

### Send

To send data to the work queue, just send a JSON message to the main address of the module. The JSON message can have any structure you like - the work queue does not look at it.

Once the work has been sent out to a worker, and processed, and that worker has replied, the reply will be forwarded back to the sender.

You can optionally receive a reply when the work has been accepted (i.e. queued, but not yet processed), to do this add a field `accepted_reply` with a value holding the address where you want the reply sent. Once the send has been accepted, and queued a message will be sent to that address:

    {
        "status": "accepted"
    }
    
If a problem occurs with the queueing, an error reply will be sent to the `accepted_reply` address (if any).

    {
        "status": "error"
        "message": <message>
    }
    
Where `message` is an error message.    

### Register

This is how a processor registers with the work queue. A processor is just an arbitrary handler on the event bus. To register itselfs as a processor, a JSON message is sent to the address given by the main address of the module + `.register`. For example if the main address is `test.orderQueue`, you send the message to `test.orderQueue.register`.

The message should have the following structure:

    {
        "processor": <processor>
    }

Where `processor` is the address of the processors handler. For example, if the processor has registered a handler at address `processor1`, then it would send the message:

    {
        "processor": "processor1"
    }    

When this message is received at the work queue, the work queue registers this address as a processor interested in work. When work arrives it will send the work off to any available processors, in a round-robin fashion.

When a processor receives some work, and has completed its processing. It should reply to the message with an empty reply. This tells the work queue that the work has been processed and can be forgotten about. If a reply is not received within `process_timeout` milliseconds then it will be assumed that the processor failed, and the work will be made available for other processors to process.

Once the register is complete, a reply message will be sent:

    {
        "status": "ok"
    }

### Unregister

This is how a processor unregisters with the work queue. To unregister itselfs as a processor, a JSON message is sent to the address given by the main address of the module + `.unregister`. For example if the main address is `test.orderQueue`, you send the message to `test.orderQueue.unregister`.

The message should have the following structure:

    {
        "processor": <processor>
    }

Once the work queue receives the message, the processor will be unregistered and will receive no more work from the queue.

Once the unregister is complete, a reply message will be sent:

    {
        "status": "ok"
    }

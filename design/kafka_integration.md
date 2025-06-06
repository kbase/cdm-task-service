# Kafka integration

This document lays out high level integration possibilities for Kafka, ranging in reliability
and complexity of implementation.

The intent of the integration is to send a message when a job's state changes -
`upload_submitted` -> `complete`, for example.

## Background

Downstream processes need to know when CTS jobs finish so they can process the job output.
As such, we want the CTS to send messages to Kafka so that downstream processes can
consume the Kafka stream and kick off ETL pipelines that consume the job output and load
(possibility transformed) data into the CDM data lake.

Importantly, the CTS should be decoupled from these processes and not send process specific
information, as that means that new processes can easily be added to the stream without needing
changes in the CTS - e.g. standard separation of concerns.

The general operations for the interactions between the CTS and downstream processes are:

* A CTS job changes state.
* The CTS sends a message with the new state, time of change, and job ID to kafka.
* (Possibly multiple) downstream processors read the Kafka stream and get the message.
* The processors pull the job record from the CTS.
    * This will necessitate the processors having a CTS admin token.
        * We may want to make more granular roles for the admin token.
* The processors will map the job information to a particular pipeline and run said pipeline.
* The processors will advance the Kafka partition pointer to the next message and repeat.

The current question is how the CTS should send the message to Kafka - there are tradeoffs
between reliability and complexity of the implementation.

## Options

### Fire and forget

By default, when submitting an event to a Kafka client, the client doesn't send the event
immediately, but rather adds it to an internal batch which is sent later. Thus if there is an
error it probably is only visible in the server logs.

#### Pros

* Simple implementation

#### Cons

* Large number of messages can be lost without anyone noticing

### Synchronous

In this case the client is configured to block until the message is sent, so any errors
are thrown and will be propagated to the caller.

#### Pros

* Reasonably simple implementation
* Users will see errors if Kafka message aren't being sent, meaning remediative actions can be
  taken quickly
  
#### Cons

* Messages can be lost
* Slight performance hit

#### Notes

The KBase Workspace, Groups, and Sample services take this approach.

### Background processor - job only

In this case, a tag is added to jobs that need notification. As well as attempting to send
the notification immediately after a state change, a background process periodically looks for
jobs needing the notification to be sent and sends said notifications if the request is
older than a specified time delta. Once the notification is successfully sent, the tag is removed.

Errors are not propagated to the caller.

#### Pros

* Messages are guaranteed to be sent eventually

#### Cons

* Messages for intermediate job state transitions may not be sent if they occur in between
  two runs of the background process
* The same message may be sent more than once if the message is sent successfully but the
  tag is not removed due to a subsequent failure
* Significantly more complex than the previous option

### Background processor - state transitions

This case is similar to the previous case except the tags are added to every state transition
rather than only the job.

#### Pros

* Messages are guaranteed to be sent eventually
* It is not possible for intermediate state notifications to be skipped

#### Cons

* The same message may be sent more than once if the message is sent successfully but the
  tag is not removed due to a subsequent failure
* The most complex option

## Decision

* On 25/3/17 it was decided that guaranteed eventual delivery of messages is required.
    * This does mean that duplicate message delivery is possible.
* It was also decided that we'll send all state transitions unless the implementation turns out
  to be more complex than expected.
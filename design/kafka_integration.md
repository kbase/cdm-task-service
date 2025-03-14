# Kafka integration

This document lays out high level integration possibilities for Kafka, ranging in reliability
and complexity of implementation.

The intent of the integration is to send a message when a job's state changes -
`upload_submitted` -> `complete`, for example.

## Fire and forget

By default, when submitting an event to a Kafka client, the client doesn't send the event
immediately, but rather adds it to an internal batch which is sent later. Thus if there is an
error it probably is only visible in the server logs.

### Pros

* Simple implementation

### Cons

* Large number of messages can be lost without anyone noticing

## Synchronous

In this case the client is configured to block until the message is sent, so any errors
are thrown and will be propagated to the caller.

### Pros

* Reasonably simple implementation
* Users will see errors if Kafka message aren't being sent, meaning remediative actions can be
  taken quickly
  
### Cons

* Messages can be lost
* Slight performance hit

### Notes

The KBase Workspace, Groups, and Sample services take this approach.

## Background processor - job only

In this case, a tag is added to jobs that need notification. As well as attempting to send
the notification immediately after a state change, a background process periodically looks for
jobs needing the notification to be sent and sends said notifications if the request is
older than a specified time delta. Once the notification is successfully sent, the tag is removed.

Errors are not propagated to the caller.

### Pros

* Messages are guaranteed to be sent eventually

### Cons

* Messages for intermediate job state transitions may not be sent if they occur in between
  two runs of the background process
* The same message may be sent more than once if the message is sent successfully but the
  tag is not removed due to a subsequent failure
* Significantly more complex than the previous option

## Background processor - state transitions

This case is similar to the previous case except the tags are added to every state transition
rather than only the job.

### Pros

* Messages are guaranteed to be sent eventually
* It is not possible for intermediate state notifications to be skipped

### Cons

* The same message may be sent more than once if the message is sent successfully but the
  tag is not removed due to a subsequent failure
* The most complex option
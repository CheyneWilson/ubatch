# Micro-batching library

TODO: Overview of sections

## Outline

Micro-batching is a technique used in processing pipelines where individual tasks are grouped together into small 
batches. This can improve throughput by reducing the number of requests made to a downstream system. Your task is to 
implement a micro-batching library, with the following requirements:
 * it should allow the caller to submit a single Job, and it should return a JobResult
 * it should process accepted Jobs in batches using a BatchProcessor
 * Don't implement BatchProcessor. This should be a dependency of your library.
 * it should provide a way to configure the batching behaviour i.e. size and frequency
 * it should expose a shutdown method which returns after all previously accepted Jobs are processed

We will be looking for:
 * a well designed API
 * usability as an external library
 * good documentation/comments
 * a well written, maintainable test suite

 The requirements above leave some unanswered questions, so you will need to use your judgement to design the most 
 useful library. Our language preference is Go, but we want to see your best effort, so please use whatever language you
 feel most comfortable with for this task.
 
## Interpretation

The purpose of the library is to group individual requests (job) into batches. These will be submitted to the downstream
service when either of the following conditions are met
 * A batch size reaches a certain size
 * Periodically, regardless of batch size

# Usage
 
TODO: explain how to use it

# Design / Structure

## Input

## Config

TODO: Explain the config approach


# Testing

## Approach

TODO: Explain why util/perf was created, and the role is serves. This should be evident when the tests are read 
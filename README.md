# Micro-batching library

TODO: Overview of sections

## Outline

The purpose of the library is to group individual requests (job) into batches. These will be submitted to the downstream
service when either of the following conditions are met
 * A batch size reaches a certain size
 * Periodically, regardless of batch size

# Usage

This library is designed for use with a `BatchProcessor[T, R]` client. 

## Examples
TODO: explain how to use it. There are some examples

# Design / Structure

## Input

## Config

TODO: Explain the config approach


# Testing

## Approach

TODO: Explain why util/perf was created, and the role is serves. This should be evident when the tests are read


# Future enhancements

Below is a list of potential future enhancements

1. **Reconfigurable micro-batcher**. Currently, the config is passed to the constructor. A small enhancement would be
to allow this to be dynamically changed.

// TODO: test for duplicate IDs, need to add this functionality in
// TODO: allow the trigger config to be changed/updated (put this in future scope)

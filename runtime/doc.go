// Package processor runs WiseIoT Access Observation Processors. A Processor owns a processor
// through a session of the Processor Runtime API of wiz-acs-core and calls a Handler for the
// observations that match the processor's filter.
//
// Every feed partition reads its pages on its own, waiting up to LongPollTimeout while it has
// nothing new, so an idle partition never delays a busy one, and memory holds at most one page per
// partition. Within a partition, the Handler sees the observations in offset order, one call at a
// time; up to MaxConcurrentPartitions partitions call it at the same time. A page of a partition is
// committed only once every observation of it has resolved, and delivery is at least once: after a
// retry, a crash or a session change, observations are handled again, so applications make their
// effects idempotent, for example with an Inbox through Idempotent.
//
// One session at a time owns a processor. Run opens a new session whenever the current one is
// fenced, by a filter change, a pause, a checkpoint reset, its expiry or a takeover, and waits as a
// standby while another instance owns the processor. Run returns an *Error that retrying cannot
// fix, such as a data gap, which an operator resolves by resetting the checkpoints over REST.
//
// Every call presents an Access API token from Config.Credential, over TLS unless
// Config.AllowInsecure, which only a loopback endpoint may use.
package processor

package jetstream

import (
	"time"
)

type watchOptFn func(opts *watchOpts) error

func (opt watchOptFn) configureWatcher(opts *watchOpts) error {
	_ = "STUB: not implemented"
	return nil
}

func IncludeHistory() WatchOpt { _ = "STUB: not implemented"; return *new(WatchOpt) }

func UpdatesOnly() WatchOpt { _ = "STUB: not implemented"; return *new(WatchOpt) }

func IgnoreDeletes() WatchOpt { _ = "STUB: not implemented"; return *new(WatchOpt) }

func MetaOnly() WatchOpt { _ = "STUB: not implemented"; return *new(WatchOpt) }

func ResumeFromRevision(revision uint64) WatchOpt { _ = "STUB: not implemented"; return *new(WatchOpt) }

type DeleteMarkersOlderThan time.Duration

func (ttl DeleteMarkersOlderThan) configurePurge(opts *purgeOpts) error {
	_ = "STUB: not implemented"
	return nil
}

type deleteOptFn func(opts *deleteOpts) error

func (opt deleteOptFn) configureDelete(opts *deleteOpts) error {
	_ = "STUB: not implemented"
	return nil
}

func LastRevision(revision uint64) KVDeleteOpt { _ = "STUB: not implemented"; return *new(KVDeleteOpt) }

func PurgeTTL(ttl time.Duration) KVDeleteOpt { _ = "STUB: not implemented"; return *new(KVDeleteOpt) }

type createOptFn func(opts *createOpts) error

func (opt createOptFn) configureCreate(opts *createOpts) error {
	_ = "STUB: not implemented"
	return nil
}

func KeyTTL(ttl time.Duration) KVCreateOpt { _ = "STUB: not implemented"; return *new(KVCreateOpt) }

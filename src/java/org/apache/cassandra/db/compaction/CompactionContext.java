package org.apache.cassandra.db.compaction;

import java.util.Objects;
import java.util.Optional;

import com.google.common.util.concurrent.RateLimiter;

import org.apache.cassandra.db.ColumnFamilyStore;

/**
 * Immutable bundle of dependencies shared by {@link CompactionWorkScheduler} implementations.
 */
public final class CompactionContext
{
    private final ColumnFamilyStore cfs;
    private final CompactionManager manager;
    private final long gcBefore;
    private final RateLimiter rateLimiter;
    private final Optional<Object> attachment;

    private CompactionContext(Builder builder)
    {
        this.cfs = builder.cfs;
        this.manager = builder.manager;
        this.gcBefore = builder.gcBefore;
        this.rateLimiter = builder.rateLimiter;
        this.attachment = Optional.ofNullable(builder.attachment);
    }

    public ColumnFamilyStore columnFamilyStore()
    {
        return cfs;
    }

    public CompactionManager manager()
    {
        return manager;
    }

    public long gcBefore()
    {
        return gcBefore;
    }

    public RateLimiter rateLimiter()
    {
        return rateLimiter;
    }

    /**
     * Allows schedulers to exchange optional metadata without expanding the core API surface.
     */
    public Optional<Object> attachment()
    {
        return attachment;
    }

    public static Builder builder(ColumnFamilyStore cfs)
    {
        return new Builder(cfs);
    }

    public static final class Builder
    {
        private final ColumnFamilyStore cfs;
        private CompactionManager manager;
        private long gcBefore;
        private RateLimiter rateLimiter;
        private Object attachment;

        private Builder(ColumnFamilyStore cfs)
        {
            this.cfs = Objects.requireNonNull(cfs, "ColumnFamilyStore is required");
        }

        public Builder withManager(CompactionManager manager)
        {
            this.manager = manager;
            return this;
        }

        public Builder withGcBefore(long gcBefore)
        {
            this.gcBefore = gcBefore;
            return this;
        }

        public Builder withRateLimiter(RateLimiter rateLimiter)
        {
            this.rateLimiter = rateLimiter;
            return this;
        }

        public Builder withAttachment(Object attachment)
        {
            this.attachment = attachment;
            return this;
        }

        public CompactionContext build()
        {
            Objects.requireNonNull(manager, "CompactionManager is required");
            Objects.requireNonNull(rateLimiter, "RateLimiter is required");
            return new CompactionContext(this);
        }
    }
}

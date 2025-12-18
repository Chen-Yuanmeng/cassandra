package org.apache.cassandra.db.compaction;

import java.util.Optional;

import org.apache.cassandra.db.ColumnFamilyStore;

/**
 * Strategy interface that encapsulates how compaction work is selected for execution.
 */
public interface CompactionWorkScheduler
{
    /**
     * Attempts to build the next {@link CompactionCommand} for the provided table and context.
     *
     * @return a populated command when work is available, otherwise {@link Optional#empty()}.
     */
    Optional<CompactionCommand> maybeBuild(ColumnFamilyStore cfs, CompactionContext context);

    /**
     * @return a descriptive scheduler name for logging and debugging.
     */
    default String name()
    {
        return getClass().getSimpleName();
    }
}

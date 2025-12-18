package org.apache.cassandra.db.compaction;

import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;

/**
 * Produces background compaction commands when capacity and configuration allow it.
 */
class BackgroundCompactionScheduler implements CompactionWorkScheduler
{
    private static final Logger logger = LoggerFactory.getLogger(BackgroundCompactionScheduler.class);

    private final CompactionManager manager;

    BackgroundCompactionScheduler(CompactionManager manager)
    {
        this.manager = manager;
    }

    @Override
    public Optional<CompactionCommand> maybeBuild(ColumnFamilyStore cfs, CompactionContext context)
    {
        if (cfs.isAutoCompactionDisabled())
        {
            logger.trace("Autocompaction is disabled");
            return Optional.empty();
        }

        int outstanding = manager.getBackgroundSubmissionCount(cfs);
        if (outstanding > 0 && manager.isCompactionExecutorAtCapacity())
        {
            logger.trace("Background compaction is still running for {}.{} ({} remaining). Skipping",
                         cfs.getKeyspaceName(), cfs.name, outstanding);
            return Optional.empty();
        }

        logger.trace("Scheduling a background task check for {}.{} with {}",
                     cfs.getKeyspaceName(),
                     cfs.name,
                     cfs.getCompactionStrategyManager().getName());

        manager.trackBackgroundSubmission(cfs);
        CompactionCommand command = new BackgroundCompactionCommand(context, manager);
        return Optional.of(command);
    }
}

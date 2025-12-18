package org.apache.cassandra.db.compaction;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;

/**
 * Executes the traditional background compaction flow, including the optional SSTable upgrade
 * fallback when no strategy-provided work is available.
 */
class BackgroundCompactionCommand extends AbstractCompactionCommand
{
    private static final Logger logger = LoggerFactory.getLogger(BackgroundCompactionCommand.class);

    private final CompactionManager manager;
    private final long gcBefore;
    private volatile boolean ranCompaction;

    BackgroundCompactionCommand(CompactionContext context, CompactionManager manager)
    {
        super(context.columnFamilyStore());
        this.manager = manager;
        this.gcBefore = context.gcBefore();
    }

    @Override
    protected void run(ActiveCompactions activeCompactions) throws Exception
    {
        boolean ran = false;
        try
        {
            ColumnFamilyStore cfs = getColumnFamilyStore();
            logger.trace("Checking {}.{}", cfs.getKeyspaceName(), cfs.name);
            if (!cfs.isValid())
            {
                logger.trace("Aborting compaction for dropped CF");
                return;
            }

            CompactionStrategyManager strategy = cfs.getCompactionStrategyManager();
            AbstractCompactionTask task = strategy.getNextBackgroundTask(gcBefore);
            if (task == null)
            {
                if (DatabaseDescriptor.automaticSSTableUpgrade())
                    ran = maybeRunUpgradeTask(strategy);
            }
            else
            {
                task.execute(activeCompactions);
                ran = true;
            }
        }
        finally
        {
            ranCompaction = ran;
            manager.untrackBackgroundSubmission(getColumnFamilyStore());
        }
    }

    @Override
    public boolean shouldReschedule()
    {
        return ranCompaction;
    }

    @Override
    public void onCancelled()
    {
        manager.untrackBackgroundSubmission(getColumnFamilyStore());
    }

    @VisibleForTesting
    boolean maybeRunUpgradeTask(CompactionStrategyManager strategy)
    {
        ColumnFamilyStore cfs = getColumnFamilyStore();
        logger.debug("Checking for upgrade tasks {}.{}", cfs.getKeyspaceName(), cfs.getTableName());
        try
        {
            if (manager.currentlyBackgroundUpgrading.incrementAndGet() <= DatabaseDescriptor.maxConcurrentAutoUpgradeTasks())
                {
                    AbstractCompactionTask upgradeTask = strategy.findUpgradeSSTableTask();
                    if (upgradeTask != null)
                    {
                        upgradeTask.execute(manager.active);
                    return true;
                }
            }
        }
        finally
        {
            manager.currentlyBackgroundUpgrading.decrementAndGet();
        }
        return false;
    }
}

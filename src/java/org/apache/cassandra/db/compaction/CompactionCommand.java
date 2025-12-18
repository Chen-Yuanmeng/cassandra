package org.apache.cassandra.db.compaction;

import org.apache.cassandra.db.ColumnFamilyStore;


/**
 * Represents a single executable unit of compaction work. Implementations are expected to be
 * instantiated by {@link CompactionWorkScheduler}s and submitted to the {@link CompactionManager}
 * for execution.
 */
public interface CompactionCommand
{
    /**
     * @return the {@link ColumnFamilyStore} this command operates on.
     */
    ColumnFamilyStore getColumnFamilyStore();

    /**
     * Performs any lightweight preparation needed before execution (for example acquiring locks or
     * validating preconditions).
     *
     * @return {@code true} when the command is ready to run, {@code false} if it should be skipped.
     */
    boolean prepare();

    /**
     * Executes the command and registers it with {@link ActiveCompactions} for observability.
     */
    void execute(ActiveCompactions activeCompactions) throws Exception;

    /**
     * @return a human readable description that can be surfaced via logs or JMX.
     */
    default String description()
    {
        return getClass().getSimpleName();
    }

    /**
     * @return {@code true} when the caller should immediately reschedule background work after this
     * command completes successfully.
     */
    default boolean shouldReschedule()
    {
        return false;
    }

    /**
     * Invoked when the command will not run (for example when the executor rejects it) so it can
     * release any resources acquired during scheduling.
     */
    default void onCancelled()
    {
    }
}

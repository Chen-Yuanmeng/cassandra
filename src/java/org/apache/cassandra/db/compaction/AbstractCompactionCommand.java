package org.apache.cassandra.db.compaction;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.cassandra.db.ColumnFamilyStore;

/**
 * Base implementation of {@link CompactionCommand} that handles one-time preparation and execution
 * semantics. Subclasses only need to implement the compaction-specific behavior.
 */
public abstract class AbstractCompactionCommand implements CompactionCommand
{
    private final ColumnFamilyStore cfs;
    private final AtomicBoolean prepared = new AtomicBoolean(false);
    private final AtomicBoolean executed = new AtomicBoolean(false);

    protected AbstractCompactionCommand(ColumnFamilyStore cfs)
    {
        this.cfs = Objects.requireNonNull(cfs, "ColumnFamilyStore is required");
    }

    @Override
    public ColumnFamilyStore getColumnFamilyStore()
    {
        return cfs;
    }

    @Override
    public final boolean prepare()
    {
        if (prepared.get())
            return true;

        boolean preparedSuccessfully = doPrepare();
        if (preparedSuccessfully)
            prepared.set(true);
        return preparedSuccessfully;
    }

    @Override
    public final void execute(ActiveCompactions activeCompactions) throws Exception
    {
        if (!prepared.get())
            throw new IllegalStateException(description() + " has not been prepared");

        if (!executed.compareAndSet(false, true))
            throw new IllegalStateException(description() + " has already executed");

        run(activeCompactions);
    }

    /**
     * Template hook invoked exactly once before {@link #execute(ActiveCompactions)} is allowed to
     * run. Implementations should allocate resources or validate the prerequisites and return
     * {@code false} if the command should be skipped.
     */
    protected boolean doPrepare()
    {
        return true;
    }

    /**
     * Template hook that performs the actual work while registered with
     * {@link ActiveCompactions}.
     */
    protected abstract void run(ActiveCompactions activeCompactions) throws Exception;
}

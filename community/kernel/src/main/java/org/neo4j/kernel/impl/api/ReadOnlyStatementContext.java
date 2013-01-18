package org.neo4j.kernel.impl.api;

import org.neo4j.graphdb.NotInTransactionException;
import org.neo4j.kernel.api.LabelNotFoundException;
import org.neo4j.kernel.api.StatementContext;

public class ReadOnlyStatementContext implements StatementContext
{
    private final StatementContext actual;

    public ReadOnlyStatementContext( StatementContext actual )
    {
        this.actual = actual;
    }

    @Override
    public long getOrCreateLabelId( String label )
    {
        throw readOnlyException();
    }

    @Override
    public void addLabelToNode( long labelId, long nodeId )
    {
        throw readOnlyException();
    }

    @Override
    public long getLabelId( String label ) throws LabelNotFoundException
    {
        return actual.getLabelId( label );
    }

    @Override
    public boolean isLabelSetOnNode( long labelId, long nodeId )
    {
        return actual.isLabelSetOnNode( labelId, nodeId );
    }

    @Override
    public void close()
    {
        actual.close();
    }

    private NotInTransactionException readOnlyException()
    {
        return new NotInTransactionException( "You have to be in a transaction context to perform write operations." );
    }
}

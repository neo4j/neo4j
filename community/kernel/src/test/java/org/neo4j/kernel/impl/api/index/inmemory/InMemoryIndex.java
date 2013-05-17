package org.neo4j.kernel.impl.api.index.inmemory;

import java.io.IOException;

import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.index.NodePropertyUpdate;

abstract class InMemoryIndex
{
    private InternalIndexState state = InternalIndexState.POPULATING;

    abstract IndexPopulator getPopulator();
    abstract IndexAccessor getOnlineAccessor();

    abstract void add( long nodeId, Object propertyValue ) throws IndexEntryConflictException, IOException;
    abstract void remove( long nodeId, Object propertyValue );

    protected abstract class Populator implements IndexPopulator
    {
        @Override
        public void create()
        {
            clear();
        }

        @Override
        public void add( long nodeId, Object propertyValue ) throws IndexEntryConflictException, IOException
        {
            InMemoryIndex.this.add( nodeId, propertyValue );
        }

        @Override
        public void update( Iterable<NodePropertyUpdate> updates ) throws IndexEntryConflictException, IOException
        {
            InMemoryIndex.this.update( updates );
        }

        @Override
        public void drop() throws IOException
        {
            InMemoryIndex.this.drop();
        }

        @Override
        public void close( boolean populationCompletedSuccessfully ) throws IOException
        {
            if ( populationCompletedSuccessfully )
            {
                state = InternalIndexState.ONLINE;
            }
        }
    }

    protected abstract class OnlineAccessor implements IndexAccessor
    {
        @Override
        public void recover( Iterable<NodePropertyUpdate> updates ) throws IOException
        {
            InMemoryIndex.this.recover( updates );
        }

        @Override
        public void updateAndCommit( Iterable<NodePropertyUpdate> updates )
                throws IOException, IndexEntryConflictException
        {
            InMemoryIndex.this.update( updates );
        }

        @Override
        public void force() throws IOException
        {
        }

        @Override
        public void drop() throws IOException
        {
            InMemoryIndex.this.drop();
        }

        @Override
        public void close() throws IOException
        {
        }
    }

    protected void update( Iterable<NodePropertyUpdate> updates ) throws IndexEntryConflictException, IOException
    {
        for ( NodePropertyUpdate update : updates )
        {
            switch ( update.getUpdateMode() )
            {
                case ADDED:
                    add( update.getNodeId(), update.getValueAfter() );
                    break;
                case CHANGED:
                    remove( update.getNodeId(), update.getValueBefore() );
                    add( update.getNodeId(), update.getValueAfter() );
                    break;
                case REMOVED:
                    remove( update.getNodeId(), update.getValueBefore() );
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
        }
    }


    protected void drop()
    {
        clear();
    }


    protected void recover( Iterable<NodePropertyUpdate> updates ) throws IOException
    {
        try
        {
            update( updates );
        }
        catch ( IndexEntryConflictException e )
        {
            throw new IllegalStateException( "Should not report index entry conflicts during recovery!", e );
        }
    }

    abstract void clear();

    InternalIndexState getState()
    {
        return state;
    }
}

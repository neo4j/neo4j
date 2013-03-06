/**
 * Copyright (c) 2002-2013 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.api;

import org.neo4j.kernel.api.ConstraintViolationKernelException;
import org.neo4j.kernel.api.IndexNotFoundKernelException;
import org.neo4j.kernel.api.InternalIndexState;
import org.neo4j.kernel.api.LabelNotFoundKernelException;
import org.neo4j.kernel.api.PropertyKeyIdNotFoundException;
import org.neo4j.kernel.api.PropertyKeyNotFoundException;
import org.neo4j.kernel.api.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.StatementContext;
import org.neo4j.kernel.impl.nioneo.store.IndexRule;

public class InteractionStoppingStatementContext implements StatementContext
{
    private final StatementContext delegate;
    private boolean closed;

    public InteractionStoppingStatementContext( StatementContext delegate )
    {
        this.delegate = delegate;
    }

    @Override
    public long getOrCreateLabelId( String label ) throws ConstraintViolationKernelException
    {
        assertOperationsAllowed();
        return delegate.getOrCreateLabelId( label );
    }

    @Override
    public long getLabelId( String label ) throws LabelNotFoundKernelException
    {
        assertOperationsAllowed();
        return delegate.getLabelId( label );
    }

    @Override
    public boolean addLabelToNode( long labelId, long nodeId )
    {
        assertOperationsAllowed();
        return delegate.addLabelToNode( labelId, nodeId );
    }

    @Override
    public boolean isLabelSetOnNode( long labelId, long nodeId )
    {
        assertOperationsAllowed();
        return delegate.isLabelSetOnNode( labelId, nodeId );
    }
    
    @Override
    public Iterable<Long> getLabelsForNode( long nodeId )
    {
        assertOperationsAllowed();
        return delegate.getLabelsForNode( nodeId );
    }
    
    @Override
    public String getLabelName( long labelId ) throws LabelNotFoundKernelException
    {
        assertOperationsAllowed();
        return delegate.getLabelName( labelId );
    }

    @Override
    public boolean removeLabelFromNode( long labelId, long nodeId )
    {
        assertOperationsAllowed();
        return delegate.removeLabelFromNode( labelId, nodeId );
    }

    @Override
    public void close( boolean successful )
    {
        assertOperationsAllowed();
        closed = true;
        delegate.close( successful );
    }
    
    @Override
    public Iterable<Long> getNodesWithLabel( long labelId )
    {
        assertOperationsAllowed();
        return delegate.getNodesWithLabel( labelId );
    }

    @Override
    public IndexRule addIndexRule( long labelId, long propertyKey ) throws ConstraintViolationKernelException
    {
        assertOperationsAllowed();
        return delegate.addIndexRule( labelId, propertyKey );
    }

    @Override
    public IndexRule getIndexRule( long labelId, long propertyKey ) throws SchemaRuleNotFoundException
    {
        assertOperationsAllowed();
        return delegate.getIndexRule( labelId, propertyKey );
    }

    @Override
    public void dropIndexRule( IndexRule indexRule ) throws ConstraintViolationKernelException
    {
        assertOperationsAllowed();
        delegate.dropIndexRule( indexRule );
    }

    @Override
    public Iterable<IndexRule> getIndexRules( long labelId )
    {
        assertOperationsAllowed();
        return delegate.getIndexRules( labelId );
    }
    
    @Override
    public Iterable<IndexRule> getIndexRules()
    {
        assertOperationsAllowed();
        return delegate.getIndexRules();
    }

    @Override
    public InternalIndexState getIndexState( IndexRule indexRule ) throws IndexNotFoundKernelException
    {
        assertOperationsAllowed();
        return delegate.getIndexState( indexRule );
    }

    @Override
    public long getOrCreatePropertyKeyId( String propertyKey )
    {
        assertOperationsAllowed();
        return delegate.getOrCreatePropertyKeyId( propertyKey );
    }

    @Override
    public long getPropertyKeyId( String propertyKey ) throws PropertyKeyNotFoundException
    {
        assertOperationsAllowed();
        return delegate.getPropertyKeyId( propertyKey );
    }

    @Override
    public String getPropertyKeyName( long propertyId ) throws PropertyKeyIdNotFoundException
    {
        assertOperationsAllowed();
        return delegate.getPropertyKeyName( propertyId );
    }

    private void assertOperationsAllowed()
    {
        if ( closed )
            throw new IllegalStateException( "This StatementContext has been closed. No more interaction allowed" );
    }

    public boolean isOpen()
    {
        return !closed;
    }
}

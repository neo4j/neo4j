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
package org.neo4j.kernel.api.operations;

import java.io.Closeable;
import java.io.IOException;
import java.util.Set;

import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.PropertyNotFoundException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.index.IndexReader;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.impl.api.DiffSets;
import org.neo4j.kernel.impl.api.IndexReaderFactory;
import org.neo4j.kernel.impl.api.LockHolder;
import org.neo4j.kernel.impl.api.index.IndexDescriptor;
import org.neo4j.kernel.impl.api.state.NodeState;
import org.neo4j.kernel.impl.api.state.TxState;

public class WritableStatementState implements StatementState
{
    private LockHolder lockHolder = NO_LOCKS;
    private TxState txState = NO_STATE;
    private IndexReaderFactory indexReaderFactory = NO_INDEX_READER_FACTORY;

    public void provide( LockHolder lockHolder )
    {
        this.lockHolder = lockHolder;
    }
    
    public void provide( TxState txState )
    {
        this.txState = txState;
    }
    
    public void provide( IndexReaderFactory indexReaderFactory )
    {
        this.indexReaderFactory = indexReaderFactory;
    }
    
    @Override
    public LockHolder locks()
    {
        return lockHolder;
    }

    @Override
    public TxState txState()
    {
        return txState;
    }
    
    @Override
    public IndexReaderFactory indexReaderFactory()
    {
        return indexReaderFactory;
    }
    
    @Override
    public void markAsClosed()
    {
    }
    
    @Override
    public Closeable closeable( final LifecycleOperations logic )
    {
        return new Closeable()
        {
            @Override
            public void close() throws IOException
            {
                logic.close( WritableStatementState.this );
            }
        };
    }
    
    private static final LockHolder NO_LOCKS = new LockHolder()
    {
        @Override
        public void releaseLocks()
        {
            throw placeHolderException();
        }
        
        @Override
        public void acquireSchemaWriteLock()
        {
            throw placeHolderException();
        }
        
        @Override
        public void acquireSchemaReadLock()
        {
            throw placeHolderException();
        }
        
        @Override
        public void acquireRelationshipWriteLock( long relationshipId )
        {
            throw placeHolderException();
        }
        
        @Override
        public void acquireRelationshipReadLock( long relationshipId )
        {
            throw placeHolderException();
        }
        
        @Override
        public void acquireNodeWriteLock( long nodeId )
        {
            throw placeHolderException();
        }
        
        @Override
        public void acquireNodeReadLock( long nodeId )
        {
            throw placeHolderException();
        }
        
        @Override
        public void acquireGraphWriteLock()
        {
            throw placeHolderException();
        }
    };
    private static final TxState NO_STATE = new TxState()
    {
        @Override
        public boolean unRemoveConstraint( UniquenessConstraint constraint )
        {
            throw placeHolderException();
        }
        
        @Override
        public void relationshipReplaceProperty( long relationshipId, Property replacedProperty, Property newProperty )
                throws PropertyNotFoundException, EntityNotFoundException
        {
            throw placeHolderException();
        }
        
        @Override
        public void relationshipRemoveProperty( long relationshipId, Property removedProperty )
                throws PropertyNotFoundException, EntityNotFoundException
        {
            throw placeHolderException();
        }
        
        @Override
        public boolean relationshipIsDeletedInThisTx( long relationshipId )
        {
            throw placeHolderException();
        }
        
        @Override
        public boolean relationshipIsAddedInThisTx( long relationshipId )
        {
            throw placeHolderException();
        }
        
        @Override
        public void relationshipDelete( long relationshipId )
        {
            throw placeHolderException();
        }
        
        @Override
        public void nodeReplaceProperty( long nodeId, Property replacedProperty, Property newProperty )
                throws PropertyNotFoundException, EntityNotFoundException
        {
            throw placeHolderException();
        }
        
        @Override
        public void nodeRemoveProperty( long nodeId, Property removedProperty ) throws PropertyNotFoundException,
                EntityNotFoundException
        {
            throw placeHolderException();
        }
        
        @Override
        public void nodeRemoveLabel( long labelId, long nodeId )
        {
            throw placeHolderException();
        }
        
        @Override
        public boolean nodeIsDeletedInThisTx( long nodeId )
        {
            throw placeHolderException();
        }
        
        @Override
        public boolean nodeIsAddedInThisTx( long nodeId )
        {
            throw placeHolderException();
        }
        
        @Override
        public void nodeDelete( long nodeId )
        {
            throw placeHolderException();
        }
        
        @Override
        public void nodeAddLabel( long labelId, long nodeId )
        {
            throw placeHolderException();
        }
        
        @Override
        public boolean hasChanges()
        {
            throw placeHolderException();
        }
        
        @Override
        public void graphReplaceProperty( Property replacedProperty, Property newProperty )
                throws PropertyNotFoundException
        {
            throw placeHolderException();
        }
        
        @Override
        public void graphRemoveProperty( Property removedProperty ) throws PropertyNotFoundException
        {
            throw placeHolderException();
        }
        
        @Override
        public DiffSets<Property> getRelationshipPropertyDiffSets( long relationshipId )
        {
            throw placeHolderException();
        }
        
        @Override
        public DiffSets<Long> getNodesWithLabelChanged( long labelId )
        {
            throw placeHolderException();
        }
        
        @Override
        public Set<Long> getNodesWithLabelAdded( long labelId )
        {
            throw placeHolderException();
        }
        
        @Override
        public DiffSets<Long> getNodesWithChangedProperty( long propertyKeyId, Object value )
        {
            throw placeHolderException();
        }
        
        @Override
        public Iterable<NodeState> getNodeStates()
        {
            throw placeHolderException();
        }
        
        @Override
        public DiffSets<Long> getNodeStateLabelDiffSets( long nodeId )
        {
            throw placeHolderException();
        }
        
        @Override
        public DiffSets<Property> getNodePropertyDiffSets( long nodeId )
        {
            throw placeHolderException();
        }
        
        @Override
        public DiffSets<Long> getLabelStateNodeDiffSets( long labelId )
        {
            throw placeHolderException();
        }
        
        @Override
        public Boolean getLabelState( long nodeId, long labelId )
        {
            throw placeHolderException();
        }
        
        @Override
        public DiffSets<IndexDescriptor> getIndexDiffSetsByLabel( long labelId )
        {
            throw placeHolderException();
        }
        
        @Override
        public DiffSets<IndexDescriptor> getIndexDiffSets()
        {
            throw placeHolderException();
        }
        
        @Override
        public DiffSets<Property> getGraphPropertyDiffSets()
        {
            throw placeHolderException();
        }
        
        @Override
        public DiffSets<Long> getDeletedNodes()
        {
            throw placeHolderException();
        }
        
        @Override
        public DiffSets<IndexDescriptor> getConstraintIndexDiffSetsByLabel( long labelId )
        {
            throw placeHolderException();
        }
        
        @Override
        public DiffSets<IndexDescriptor> getConstraintIndexDiffSets()
        {
            throw placeHolderException();
        }
        
        @Override
        public void dropIndex( IndexDescriptor descriptor )
        {
            throw placeHolderException();
        }
        
        @Override
        public void dropConstraintIndex( IndexDescriptor descriptor )
        {
            throw placeHolderException();
        }
        
        @Override
        public void dropConstraint( UniquenessConstraint constraint )
        {
            throw placeHolderException();
        }
        
        @Override
        public Iterable<IndexDescriptor> createdConstraintIndexes()
        {
            throw placeHolderException();
        }
        
        @Override
        public DiffSets<UniquenessConstraint> constraintsChangesForLabelAndProperty( long labelId, long propertyKey )
        {
            throw placeHolderException();
        }
        
        @Override
        public DiffSets<UniquenessConstraint> constraintsChangesForLabel( long labelId )
        {
            throw placeHolderException();
        }
        
        @Override
        public DiffSets<UniquenessConstraint> constraintsChanges()
        {
            throw placeHolderException();
        }
        
        @Override
        public void addIndexRule( IndexDescriptor descriptor )
        {
            throw placeHolderException();
        }
        
        @Override
        public void addConstraintIndexRule( IndexDescriptor descriptor )
        {
            throw placeHolderException();
        }
        
        @Override
        public void addConstraint( UniquenessConstraint constraint, long indexId )
        {
            throw placeHolderException();
        }
        
        @Override
        public void accept( Visitor visitor )
        {
            throw placeHolderException();
        }
    };
    private static final IndexReaderFactory NO_INDEX_READER_FACTORY = new IndexReaderFactory()
    {
        @Override
        public IndexReader newReader( long indexId ) throws IndexNotFoundKernelException
        {
            throw placeHolderException();
        }
        
        @Override
        public void close()
        {
            throw placeHolderException();
        }
    };

    protected static UnsupportedOperationException placeHolderException()
    {
        return new UnsupportedOperationException( "No proper instance provided" );
    }
}

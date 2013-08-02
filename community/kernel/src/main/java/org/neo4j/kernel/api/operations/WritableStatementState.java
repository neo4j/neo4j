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
    private TxState.Holder txStateHolder = NO_STATE_HOLDER;
    private IndexReaderFactory indexReaderFactory = NO_INDEX_READER_FACTORY;

    public void provide( LockHolder lockHolder )
    {
        this.lockHolder = lockHolder;
    }
    
    public void provide( TxState.Holder txStateHolder )
    {
        this.txStateHolder = txStateHolder;
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
        return txStateHolder.txState();
    }

    @Override
    public boolean hasTxState()
    {
        return txStateHolder.hasTxState();
    }

    @Override
    public boolean hasTxStateWithChanges()
    {
        return txStateHolder.hasTxStateWithChanges();
    }

    @Override
    public IndexReaderFactory indexReaderFactory()
    {
        return indexReaderFactory;
    }
    
    @Override
    public void close()
    {
        indexReaderFactory().close();
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

    private static final TxState.Holder NO_STATE_HOLDER = new TxState.Holder() {
        @Override
        public TxState txState()
        {
            return NO_STATE;
        }

        @Override
        public boolean hasTxState()
        {
            return false;
        }

        @Override
        public boolean hasTxStateWithChanges()
        {
            return false;
        }
    };

    private static final TxState NO_STATE = new TxState()
    {
        @Override
        public boolean constraintDoUnRemove( UniquenessConstraint constraint )
        {
            throw placeHolderException();
        }
        
        @Override
        public void relationshipDoReplaceProperty( long relationshipId, Property replacedProperty,
                                                   Property newProperty )
                throws PropertyNotFoundException, EntityNotFoundException
        {
            throw placeHolderException();
        }
        
        @Override
        public void relationshipDoRemoveProperty( long relationshipId, Property removedProperty )
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
        public void relationshipDoDelete( long relationshipId )
        {
            throw placeHolderException();
        }
        
        @Override
        public void nodeDoReplaceProperty( long nodeId, Property replacedProperty, Property newProperty )
                throws PropertyNotFoundException, EntityNotFoundException
        {
            throw placeHolderException();
        }
        
        @Override
        public void nodeDoRemoveProperty( long nodeId, Property removedProperty ) throws PropertyNotFoundException,
                EntityNotFoundException
        {
            throw placeHolderException();
        }
        
        @Override
        public void nodeDoRemoveLabel( long labelId, long nodeId )
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
        public void nodeDoDelete( long nodeId )
        {
            throw placeHolderException();
        }
        
        @Override
        public void nodeDoAddLabel( long labelId, long nodeId )
        {
            throw placeHolderException();
        }
        
        @Override
        public boolean hasChanges()
        {
            throw placeHolderException();
        }
        
        @Override
        public void graphDoReplaceProperty( Property replacedProperty, Property newProperty )
                throws PropertyNotFoundException
        {
            throw placeHolderException();
        }
        
        @Override
        public void graphDoRemoveProperty( Property removedProperty ) throws PropertyNotFoundException
        {
            throw placeHolderException();
        }
        
        @Override
        public DiffSets<Property> relationshipPropertyDiffSets( long relationshipId )
        {
            throw placeHolderException();
        }
        
        @Override
        public DiffSets<Long> nodesWithLabelChanged( long labelId )
        {
            throw placeHolderException();
        }
        
        @Override
        public Set<Long> nodesWithLabelAdded( long labelId )
        {
            throw placeHolderException();
        }
        
        @Override
        public DiffSets<Long> nodesWithChangedProperty( long propertyKeyId, Object value )
        {
            throw placeHolderException();
        }
        
        @Override
        public Iterable<NodeState> nodeStates()
        {
            throw placeHolderException();
        }
        
        @Override
        public DiffSets<Long> nodeStateLabelDiffSets( long nodeId )
        {
            throw placeHolderException();
        }
        
        @Override
        public DiffSets<Property> nodePropertyDiffSets( long nodeId )
        {
            throw placeHolderException();
        }
        
        @Override
        public DiffSets<Long> labelStateNodeDiffSets( long labelId )
        {
            throw placeHolderException();
        }
        
        @Override
        public UpdateTriState labelState( long nodeId, long labelId )
        {
            throw placeHolderException();
        }
        
        @Override
        public DiffSets<IndexDescriptor> indexDiffSetsByLabel( long labelId )
        {
            throw placeHolderException();
        }
        
        @Override
        public DiffSets<IndexDescriptor> indexChanges()
        {
            throw placeHolderException();
        }
        
        @Override
        public DiffSets<Property> graphPropertyDiffSets()
        {
            throw placeHolderException();
        }
        
        @Override
        public DiffSets<Long> nodesDeletedInTx()
        {
            throw placeHolderException();
        }
        
        @Override
        public DiffSets<IndexDescriptor> constraintIndexDiffSetsByLabel( long labelId )
        {
            throw placeHolderException();
        }
        
        @Override
        public DiffSets<IndexDescriptor> constraintIndexChanges()
        {
            throw placeHolderException();
        }
        
        @Override
        public void indexDoDrop( IndexDescriptor descriptor )
        {
            throw placeHolderException();
        }
        
        @Override
        public void constraintIndexDoDrop( IndexDescriptor descriptor )
        {
            throw placeHolderException();
        }
        
        @Override
        public void constraintDoDrop( UniquenessConstraint constraint )
        {
            throw placeHolderException();
        }
        
        @Override
        public Iterable<IndexDescriptor> constraintIndexesCreatedInTx()
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
        public void indexRuleDoAdd( IndexDescriptor descriptor )
        {
            throw placeHolderException();
        }
        
        @Override
        public void constraintIndexRuleDoAdd( IndexDescriptor descriptor )
        {
            throw placeHolderException();
        }
        
        @Override
        public void constraintDoAdd( UniquenessConstraint constraint, long indexId )
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

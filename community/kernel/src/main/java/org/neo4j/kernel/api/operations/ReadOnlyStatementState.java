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

import java.util.Collections;
import java.util.Set;

import org.neo4j.graphdb.NotInTransactionException;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.PropertyNotFoundException;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.impl.api.DiffSets;
import org.neo4j.kernel.impl.api.IndexReaderFactory;
import org.neo4j.kernel.impl.api.LockHolder;
import org.neo4j.kernel.impl.api.index.IndexDescriptor;
import org.neo4j.kernel.impl.api.state.NodeState;
import org.neo4j.kernel.impl.api.state.TxState;

public class ReadOnlyStatementState implements StatementState
{
    private final IndexReaderFactory indexReaderFactory;

    public ReadOnlyStatementState( IndexReaderFactory indexReaderFactory )
    {
        this.indexReaderFactory = indexReaderFactory;
    }
    
    @Override
    public LockHolder locks()
    {
        return NO_LOCKS;
    }

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
            throw readOnlyTransaction();
        }
        
        @Override
        public void acquireSchemaWriteLock()
        {
            throw readOnlyTransaction();
        }
        
        @Override
        public void acquireSchemaReadLock()
        {
            // This seems to be OK. Before the refactoring where all state lives outside of StatementOperations
            // there was a read-only statement operations that had no locking layer at all
        }
        
        @Override
        public void acquireRelationshipWriteLock( long relationshipId )
        {
            throw readOnlyTransaction();
        }
        
        @Override
        public void acquireRelationshipReadLock( long relationshipId )
        {
            throw readOnlyTransaction();
        }
        
        @Override
        public void acquireNodeWriteLock( long nodeId )
        {
            throw readOnlyTransaction();
        }
        
        @Override
        public void acquireNodeReadLock( long nodeId )
        {
            throw readOnlyTransaction();
        }
        
        @Override
        public void acquireGraphWriteLock()
        {
            throw readOnlyTransaction();
        }
    };
    
    private static final TxState NO_STATE = new TxState()
    {
        @Override
        public boolean constraintDoUnRemove( UniquenessConstraint constraint )
        {
            throw readOnlyTransaction();
        }
        
        @Override
        public void relationshipDoReplaceProperty( long relationshipId, Property replacedProperty,
                                                   Property newProperty )
                throws PropertyNotFoundException, EntityNotFoundException
        {
            throw readOnlyTransaction();
        }
        
        @Override
        public void relationshipDoRemoveProperty( long relationshipId, Property removedProperty )
                throws PropertyNotFoundException, EntityNotFoundException
        {
            throw readOnlyTransaction();
        }
        
        @Override
        public boolean relationshipIsDeletedInThisTx( long relationshipId )
        {
            return false;
        }
        
        @Override
        public boolean relationshipIsAddedInThisTx( long relationshipId )
        {
            return false;
        }
        
        @Override
        public void relationshipDoDelete( long relationshipId )
        {
            throw readOnlyTransaction();
        }
        
        @Override
        public void nodeDoReplaceProperty( long nodeId, Property replacedProperty, Property newProperty )
                throws PropertyNotFoundException, EntityNotFoundException
        {
            throw readOnlyTransaction();
        }
        
        @Override
        public void nodeDoRemoveProperty( long nodeId, Property removedProperty ) throws PropertyNotFoundException,
                EntityNotFoundException
        {
            throw readOnlyTransaction();
        }
        
        @Override
        public void nodeDoRemoveLabel( long labelId, long nodeId )
        {
            throw readOnlyTransaction();
        }
        
        @Override
        public boolean nodeIsDeletedInThisTx( long nodeId )
        {
            return false;
        }
        
        @Override
        public boolean nodeIsAddedInThisTx( long nodeId )
        {
            return false;
        }
        
        @Override
        public void nodeDoDelete( long nodeId )
        {
            throw readOnlyTransaction();
        }
        
        @Override
        public void nodeDoAddLabel( long labelId, long nodeId )
        {
            throw readOnlyTransaction();
        }
        
        @Override
        public boolean hasChanges()
        {
            return false;
        }
        
        @Override
        public void graphDoReplaceProperty( Property replacedProperty, Property newProperty )
                throws PropertyNotFoundException
        {
            throw readOnlyTransaction();
        }
        
        @Override
        public void graphDoRemoveProperty( Property removedProperty ) throws PropertyNotFoundException
        {
            throw readOnlyTransaction();
        }
        
        @Override
        public DiffSets<Property> relationshipPropertyDiffSets( long relationshipId )
        {
            return DiffSets.emptyDiffSets();
        }
        
        @Override
        public DiffSets<Long> nodesWithLabelChanged( long labelId )
        {
            return DiffSets.emptyDiffSets();
        }
        
        @Override
        public Set<Long> nodesWithLabelAdded( long labelId )
        {
            return Collections.emptySet();
        }
        
        @Override
        public DiffSets<Long> nodesWithChangedProperty( long propertyKeyId, Object value )
        {
            return DiffSets.emptyDiffSets();
        }
        
        @Override
        public Iterable<NodeState> nodeStates()
        {
            return Collections.emptyList();
        }
        
        @Override
        public DiffSets<Long> nodeStateLabelDiffSets( long nodeId )
        {
            return DiffSets.emptyDiffSets();
        }
        
        @Override
        public DiffSets<Property> nodePropertyDiffSets( long nodeId )
        {
            return DiffSets.emptyDiffSets();
        }
        
        @Override
        public DiffSets<Long> labelStateNodeDiffSets( long labelId )
        {
            return DiffSets.emptyDiffSets();
        }
        
        @Override
        public UpdateTriState labelState( long nodeId, long labelId )
        {
            return UpdateTriState.UNTOUCHED;
        }
        
        @Override
        public DiffSets<IndexDescriptor> indexDiffSetsByLabel( long labelId )
        {
            return DiffSets.emptyDiffSets();
        }
        
        @Override
        public DiffSets<IndexDescriptor> indexChanges()
        {
            return DiffSets.emptyDiffSets();
        }
        
        @Override
        public DiffSets<Property> graphPropertyDiffSets()
        {
            return DiffSets.emptyDiffSets();
        }
        
        @Override
        public DiffSets<Long> nodesDeletedInTx()
        {
            return DiffSets.emptyDiffSets();
        }
        
        @Override
        public DiffSets<IndexDescriptor> constraintIndexDiffSetsByLabel( long labelId )
        {
            return DiffSets.emptyDiffSets();
        }
        
        @Override
        public DiffSets<IndexDescriptor> constraintIndexChanges()
        {
            return DiffSets.emptyDiffSets();
        }
        
        @Override
        public void indexDoDrop( IndexDescriptor descriptor )
        {
            throw readOnlyTransaction();
        }
        
        @Override
        public void constraintIndexDoDrop( IndexDescriptor descriptor )
        {
            throw readOnlyTransaction();
        }
        
        @Override
        public void constraintDoDrop( UniquenessConstraint constraint )
        {
            throw readOnlyTransaction();
        }
        
        @Override
        public Iterable<IndexDescriptor> constraintIndexesCreatedInTx()
        {
            return Collections.emptyList();
        }
        
        @Override
        public DiffSets<UniquenessConstraint> constraintsChangesForLabelAndProperty( long labelId, long propertyKey )
        {
            return DiffSets.emptyDiffSets();
        }
        
        @Override
        public DiffSets<UniquenessConstraint> constraintsChangesForLabel( long labelId )
        {
            return DiffSets.emptyDiffSets();
        }
        
        @Override
        public DiffSets<UniquenessConstraint> constraintsChanges()
        {
            return DiffSets.emptyDiffSets();
        }
        
        @Override
        public void indexRuleDoAdd( IndexDescriptor descriptor )
        {
            throw readOnlyTransaction();
        }
        
        @Override
        public void constraintIndexRuleDoAdd( IndexDescriptor descriptor )
        {
            throw readOnlyTransaction();
        }
        
        @Override
        public void constraintDoAdd( UniquenessConstraint constraint, long indexId )
        {
            throw readOnlyTransaction();
        }
        
        @Override
        public void accept( Visitor visitor )
        {
            throw readOnlyTransaction();
        }
    };

    protected static NotInTransactionException readOnlyTransaction()
    {
        return new NotInTransactionException();
    }
}

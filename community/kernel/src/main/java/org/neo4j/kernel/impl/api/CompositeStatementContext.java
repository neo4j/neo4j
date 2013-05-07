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

import java.io.Closeable;
import java.util.Iterator;

import org.neo4j.helpers.Function;
import org.neo4j.kernel.api.DataIntegrityKernelException;
import org.neo4j.kernel.api.EntityNotFoundException;
import org.neo4j.kernel.api.LabelNotFoundKernelException;
import org.neo4j.kernel.api.PropertyKeyIdNotFoundException;
import org.neo4j.kernel.api.PropertyKeyNotFoundException;
import org.neo4j.kernel.api.PropertyNotFoundException;
import org.neo4j.kernel.api.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.StatementContext;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.operations.EntityOperations;
import org.neo4j.kernel.api.operations.KeyOperations;
import org.neo4j.kernel.api.operations.SchemaOperations;
import org.neo4j.kernel.api.operations.SchemaStateOperations;
import org.neo4j.kernel.impl.api.index.IndexDescriptor;
import org.neo4j.kernel.impl.core.LabelToken;

import static org.neo4j.helpers.Exceptions.launderedException;

/**
 * This is syntax sugar, it helps implementing statement contexts that either just want to delegate
 * to some other context, or wants to split it's own implementation into multiple parts to minimize clutter.
 */
public class CompositeStatementContext implements StatementContext
{
    private final KeyOperations keyOperations;
    private final EntityOperations entityOperations;
    private final SchemaOperations schemaOperations;
    private final SchemaStateOperations schemaStateOperations;

    private final Closeable delegateToClose;

    // This class is divided into read and write operations, please help keep it that way for readability
    public CompositeStatementContext()
    {
        // If not given anything to delegate to, default to making all ops unsupported.
        this( UnsupportiveStatementContext.instance() );
    }

    public CompositeStatementContext( StatementContext delegate )
    {
        this( delegate, delegate, delegate, delegate, delegate );
    }

    public CompositeStatementContext( StatementContext delegate, SchemaStateOperations schemaStateOperations )
    {
        this( delegate, delegate, delegate, schemaStateOperations, delegate );
    }

    public CompositeStatementContext( StatementContext delegate, SchemaOperations schemaOperations )
    {
        this( delegate, delegate, schemaOperations, delegate, delegate );
    }

    private CompositeStatementContext( KeyOperations keyOperations, EntityOperations entityOperations,
                                       SchemaOperations schemaOperations, SchemaStateOperations schemaStateOperations,
                                       Closeable delegateToClose )
    {
        this.keyOperations = keyOperations;
        this.entityOperations = entityOperations;
        this.schemaOperations = schemaOperations;
        this.schemaStateOperations = schemaStateOperations;
        this.delegateToClose = delegateToClose;
    }

    // Hook methods

    protected void beforeOperation()
    {
    }

    protected void afterOperation()
    {
    }

    protected void beforeReadOperation()
    {
    }

    protected void afterReadOperation()
    {
    }

    protected void beforeWriteOperation()
    {
    }

    protected void afterWriteOperation()
    {
    }

    //
    // META OPERATIONS
    //

    @Override
    public void close()
    {
        if ( delegateToClose != null )
        {
            try
            {
                delegateToClose.close();
            }
            catch ( Exception e )
            {
                throw launderedException( "Failed to close " + this, e );
            }
        }
        else
        {
            throw new IllegalStateException( "Asked to close, but was not given a full implementation of statement " +
                    "context. Please either override this close method, or give CompositeStatementContext a full " +
                    "implementation of the statement context interface." );
        }
    }

    //
    // SCHEMA STATE OPERATIONS - these are operations, but neither read nor write operations.
    //

    @Override
    public <K> boolean schemaStateContains( K key )
    {
        beforeOperation();

        boolean result = schemaStateOperations.schemaStateContains( key );

        afterOperation();
        return result;
    }

    @Override
    public <K, V> V getOrCreateFromSchemaState( K key, Function<K, V> creator )
    {
        beforeOperation();

        V result = schemaStateOperations.getOrCreateFromSchemaState( key, creator );

        afterOperation();
        return result;
    }

    //
    // READ OPERATIONS
    //

    @Override
    public Iterator<Long> getNodesWithLabel( long labelId )
    {
        beforeOperation();
        beforeReadOperation();

        Iterator<Long> result = entityOperations.getNodesWithLabel( labelId );

        afterReadOperation();
        afterOperation();
        return result;
    }

    @Override
    public Iterator<Long> exactIndexLookup( IndexDescriptor index, Object value ) throws IndexNotFoundKernelException
    {
        beforeOperation();
        beforeReadOperation();

        Iterator<Long> result = entityOperations.exactIndexLookup( index, value );

        afterReadOperation();
        afterOperation();
        return result;
    }

    @Override
    public long getLabelId( String label ) throws LabelNotFoundKernelException
    {
        beforeOperation();
        beforeReadOperation();

        long result = keyOperations.getLabelId( label );

        afterReadOperation();
        afterOperation();
        return result;
    }

    @Override
    public String getLabelName( long labelId ) throws LabelNotFoundKernelException
    {
        beforeOperation();
        beforeReadOperation();

        String result = keyOperations.getLabelName( labelId );

        afterReadOperation();
        afterOperation();
        return result;
    }

    @Override
    public boolean isLabelSetOnNode( long labelId, long nodeId ) throws EntityNotFoundException
    {
        beforeOperation();
        beforeReadOperation();

        boolean result = entityOperations.isLabelSetOnNode( labelId, nodeId );

        afterReadOperation();
        afterOperation();
        return result;
    }

    @Override
    public Iterator<Long> getLabelsForNode( long nodeId ) throws EntityNotFoundException
    {
        beforeOperation();
        beforeReadOperation();

        Iterator<Long> result = entityOperations.getLabelsForNode( nodeId );

        afterReadOperation();
        afterOperation();
        return result;
    }

    @Override
    public long getPropertyKeyId( String propertyKey ) throws PropertyKeyNotFoundException
    {
        beforeOperation();
        beforeReadOperation();

        long result = keyOperations.getPropertyKeyId( propertyKey );

        afterReadOperation();
        afterOperation();
        return result;
    }

    @Override
    public String getPropertyKeyName( long propertyId ) throws PropertyKeyIdNotFoundException
    {
        beforeOperation();
        beforeReadOperation();

        String result = keyOperations.getPropertyKeyName( propertyId );

        afterReadOperation();
        afterOperation();
        return result;
    }

    @Override
    public Object getNodePropertyValue( long nodeId, long propertyId ) throws PropertyKeyIdNotFoundException,
            PropertyNotFoundException, EntityNotFoundException
    {
        beforeOperation();
        beforeReadOperation();

        Object result = entityOperations.getNodePropertyValue( nodeId, propertyId );

        afterReadOperation();
        afterOperation();
        return result;
    }


    @Override
    public boolean nodeHasProperty(long nodeId, long propertyId)
            throws PropertyKeyIdNotFoundException, EntityNotFoundException
    {
        beforeOperation();
        beforeReadOperation();

        boolean result = entityOperations.nodeHasProperty( nodeId, propertyId );

        afterReadOperation();
        afterOperation();
        return result;
    }

    @Override
    public Iterator<Long> listNodePropertyKeys( long nodeId )
    {
        beforeOperation();
        beforeReadOperation();

        Iterator<Long> result = entityOperations.listNodePropertyKeys( nodeId );

        afterReadOperation();
        afterOperation();
        return result;
    }

    @Override
    public Iterator<Long> listRelationshipPropertyKeys( long relationshipId )
    {
        beforeOperation();
        beforeReadOperation();

        Iterator<Long> result = entityOperations.listRelationshipPropertyKeys( relationshipId );

        afterReadOperation();
        afterOperation();
        return result;
    }

    @Override
    public IndexDescriptor getIndex( long labelId, long propertyKey ) throws SchemaRuleNotFoundException
    {
        beforeOperation();
        beforeReadOperation();

        IndexDescriptor result = schemaOperations.getIndex( labelId, propertyKey );

        afterReadOperation();
        afterOperation();
        return result;
    }

    @Override
    public Iterator<IndexDescriptor> getIndexes( long labelId )
    {
        beforeOperation();
        beforeReadOperation();

        Iterator<IndexDescriptor> result = schemaOperations.getIndexes( labelId );

        afterReadOperation();
        afterOperation();
        return result;
    }

    @Override
    public Iterator<IndexDescriptor> getIndexes()
    {
        beforeOperation();
        beforeReadOperation();

        Iterator<IndexDescriptor> result = schemaOperations.getIndexes();

        afterReadOperation();
        afterOperation();
        return result;
    }

    @Override
    public Iterator<IndexDescriptor> getConstraintIndexes( long labelId )
    {
        beforeOperation();
        beforeReadOperation();

        Iterator<IndexDescriptor> result = schemaOperations.getConstraintIndexes( labelId );

        afterReadOperation();
        afterOperation();
        return result;
    }

    @Override
    public Iterator<IndexDescriptor> getConstraintIndexes()
    {
        beforeOperation();
        beforeReadOperation();

        Iterator<IndexDescriptor> result = schemaOperations.getConstraintIndexes();

        afterReadOperation();
        afterOperation();
        return result;
    }

    @Override
    public InternalIndexState getIndexState( IndexDescriptor indexRule ) throws IndexNotFoundKernelException
    {
        beforeOperation();
        beforeReadOperation();

        InternalIndexState result = schemaOperations.getIndexState( indexRule );

        afterReadOperation();
        afterOperation();
        return result;
    }

    @Override
    public Iterator<UniquenessConstraint> getConstraints( long labelId, long propertyKeyId )
    {
        beforeOperation();
        beforeReadOperation();

        Iterator<UniquenessConstraint> result = schemaOperations.getConstraints( labelId, propertyKeyId );

        afterReadOperation();
        afterOperation();
        return result;
    }

    @Override
    public Iterator<UniquenessConstraint> getConstraints( long labelId )
    {
        beforeOperation();
        beforeReadOperation();

        Iterator<UniquenessConstraint> result = schemaOperations.getConstraints( labelId );

        afterReadOperation();
        afterOperation();
        return result;
    }

    @Override
    public Long getOwningConstraint( IndexDescriptor index ) throws SchemaRuleNotFoundException
    {
        beforeOperation();
        beforeReadOperation();

        Long result = schemaOperations.getOwningConstraint( index );

        afterReadOperation();
        afterOperation();
        return result;
    }

    @Override
    public long getCommittedIndexId( IndexDescriptor index ) throws SchemaRuleNotFoundException
    {
        beforeOperation();
        beforeReadOperation();

        long result = schemaOperations.getCommittedIndexId( index );

        afterReadOperation();
        afterOperation();
        return result;
    }

    @Override
    public Iterator<UniquenessConstraint> getConstraints()
    {
        beforeOperation();
        beforeReadOperation();

        Iterator<UniquenessConstraint> result = schemaOperations.getConstraints();

        afterReadOperation();
        afterOperation();
        return result;
    }

    @Override
    public Iterator<LabelToken> listLabels()
    {
        beforeOperation();
        beforeReadOperation();

        Iterator<LabelToken> result = keyOperations.listLabels();

        afterReadOperation();
        afterOperation();
        return result;
    }

    //
    // WRITE OPERATIONS
    //

    @Override
    public long getOrCreateLabelId( String label ) throws DataIntegrityKernelException
    {
        beforeOperation();
        beforeWriteOperation();

        long result = keyOperations.getOrCreateLabelId( label );

        afterWriteOperation();
        afterOperation();
        return result;
    }

    @Override
    public boolean addLabelToNode( long labelId, long nodeId ) throws EntityNotFoundException
    {
        beforeOperation();
        beforeWriteOperation();

        boolean result = entityOperations.addLabelToNode( labelId, nodeId );

        afterWriteOperation();
        afterOperation();
        return result;
    }

    @Override
    public boolean removeLabelFromNode( long labelId, long nodeId ) throws EntityNotFoundException
    {
        beforeOperation();
        beforeWriteOperation();

        boolean result = entityOperations.removeLabelFromNode( labelId, nodeId );

        afterWriteOperation();
        afterOperation();
        return result;
    }

    @Override
    public long getOrCreatePropertyKeyId( String propertyKey ) throws DataIntegrityKernelException
    {
        beforeOperation();
        beforeWriteOperation();

        long result = keyOperations.getOrCreatePropertyKeyId( propertyKey );

        afterWriteOperation();
        afterOperation();
        return result;
    }

    @Override
    public IndexDescriptor addIndex( long labelId, long propertyKey ) throws
                                                                      DataIntegrityKernelException
    {
        beforeOperation();
        beforeWriteOperation();

        IndexDescriptor result = schemaOperations.addIndex( labelId, propertyKey );

        afterWriteOperation();
        afterOperation();
        return result;
    }

    @Override
    public IndexDescriptor addConstraintIndex( long labelId, long propertyKey )
            throws DataIntegrityKernelException
    {
        beforeOperation();
        beforeWriteOperation();

        IndexDescriptor result = schemaOperations.addConstraintIndex( labelId, propertyKey );

        afterWriteOperation();
        afterOperation();
        return result;
    }

    @Override
    public UniquenessConstraint addUniquenessConstraint( long labelId, long propertyKeyId )
            throws DataIntegrityKernelException, ConstraintCreationKernelException
    {
        beforeOperation();
        beforeWriteOperation();

        UniquenessConstraint result = schemaOperations.addUniquenessConstraint( labelId, propertyKeyId );

        afterWriteOperation();
        afterOperation();
        return result;
    }

    @Override
    public void dropConstraint( UniquenessConstraint constraint )
    {
        beforeOperation();
        beforeWriteOperation();

        schemaOperations.dropConstraint( constraint );

        afterWriteOperation();
        afterOperation();
    }

    @Override
    public void dropIndex( IndexDescriptor descriptor ) throws DataIntegrityKernelException
    {
        beforeOperation();
        beforeWriteOperation();

        schemaOperations.dropIndex( descriptor );

        afterWriteOperation();
        afterOperation();
    }

    @Override
    public void dropConstraintIndex( IndexDescriptor descriptor ) throws DataIntegrityKernelException
    {
        beforeOperation();
        beforeWriteOperation();

        schemaOperations.dropConstraintIndex( descriptor );

        afterWriteOperation();
        afterOperation();
    }

    @Override
    public void nodeSetPropertyValue( long nodeId, long propertyKeyId, Object value )
            throws PropertyKeyIdNotFoundException, EntityNotFoundException
    {
        beforeOperation();
        beforeWriteOperation();

        entityOperations.nodeSetPropertyValue( nodeId, propertyKeyId, value );

        afterWriteOperation();
        afterOperation();
    }

    @Override
    public Object nodeRemoveProperty( long nodeId, long propertyKeyId )
            throws PropertyKeyIdNotFoundException, EntityNotFoundException
    {
        beforeOperation();
        beforeWriteOperation();

        Object result = entityOperations.nodeRemoveProperty( nodeId, propertyKeyId );

        afterWriteOperation();
        afterOperation();

        return result;
    }

    @Override
    public void deleteNode( long nodeId )
    {
        beforeOperation();
        beforeWriteOperation();

        entityOperations.deleteNode( nodeId );

        afterWriteOperation();
        afterOperation();
    }
}

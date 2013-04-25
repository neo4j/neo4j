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

import java.util.Iterator;

import org.neo4j.helpers.Function;
import org.neo4j.kernel.api.ConstraintViolationKernelException;
import org.neo4j.kernel.api.EntityNotFoundException;
import org.neo4j.kernel.api.LabelNotFoundKernelException;
import org.neo4j.kernel.api.PropertyKeyIdNotFoundException;
import org.neo4j.kernel.api.PropertyKeyNotFoundException;
import org.neo4j.kernel.api.PropertyNotFoundException;
import org.neo4j.kernel.api.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.StatementContext;
import org.neo4j.kernel.api.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.operations.EntityOperations;
import org.neo4j.kernel.api.operations.LabelOperations;
import org.neo4j.kernel.api.operations.PropertyOperations;
import org.neo4j.kernel.api.operations.SchemaOperations;
import org.neo4j.kernel.impl.api.index.IndexDescriptor;
import org.neo4j.kernel.impl.nioneo.store.IndexRule;

/**
 * This is syntax sugar, it helps implementing statement contexts that either just want to delegate
 * to some other context, or wants to split it's own implementation into multiple parts to minimize clutter.
 */
public class CompositeStatementContext implements StatementContext
{
    private final EntityOperations entityOperations;
    private final PropertyOperations propertyOperations;
    private final LabelOperations labelOperations;
    private final SchemaOperations schemaOperations;

    private final StatementContext delegateToClose;

    // This class is divided into read and write operations, please help keep it that way for readability

    public CompositeStatementContext()
    {
        // If not given anything to delegate to, default to making all ops unsupported.
        this(UnsupportiveStatementContext.instance());

    }

    public CompositeStatementContext( StatementContext delegate )
    {
        this.entityOperations = delegate;
        this.propertyOperations = delegate;
        this.labelOperations = delegate;
        this.schemaOperations = delegate;
        this.delegateToClose = delegate;
    }

    public CompositeStatementContext( StatementContext delegate, SchemaOperations schemaOperations )
    {
        this.entityOperations = delegate;
        this.propertyOperations = delegate;
        this.labelOperations = delegate;
        this.schemaOperations = schemaOperations;
        this.delegateToClose = delegate;
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
            delegateToClose.close();
        }
        else
        {
            throw new IllegalStateException( "Asked to close, but was not given a full implementation of statement " +
                    "context. Please either override this close method, or give CompositeStatementContext a full " +
                    "implementation of the statement context interface." );
        }
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
    public Iterator<Long> exactIndexLookup( long indexId, Object value ) throws IndexNotFoundKernelException
    {
        beforeOperation();
        beforeReadOperation();

        Iterator<Long> result = entityOperations.exactIndexLookup( indexId, value );

        afterReadOperation();
        afterOperation();
        return result;
    }

    @Override
    public long getLabelId( String label ) throws LabelNotFoundKernelException
    {
        beforeOperation();
        beforeReadOperation();

        long result = labelOperations.getLabelId( label );

        afterReadOperation();
        afterOperation();
        return result;
    }

    @Override
    public String getLabelName( long labelId ) throws LabelNotFoundKernelException
    {
        beforeOperation();
        beforeReadOperation();

        String result = labelOperations.getLabelName( labelId );

        afterReadOperation();
        afterOperation();
        return result;
    }

    @Override
    public boolean isLabelSetOnNode( long labelId, long nodeId ) throws EntityNotFoundException
    {
        beforeOperation();
        beforeReadOperation();

        boolean result = labelOperations.isLabelSetOnNode( labelId, nodeId );

        afterReadOperation();
        afterOperation();
        return result;
    }

    @Override
    public Iterator<Long> getLabelsForNode( long nodeId ) throws EntityNotFoundException
    {
        beforeOperation();
        beforeReadOperation();

        Iterator<Long> result = labelOperations.getLabelsForNode( nodeId );

        afterReadOperation();
        afterOperation();
        return result;
    }

    @Override
    public long getPropertyKeyId( String propertyKey ) throws PropertyKeyNotFoundException
    {
        beforeOperation();
        beforeReadOperation();

        long result = propertyOperations.getPropertyKeyId( propertyKey );

        afterReadOperation();
        afterOperation();
        return result;
    }

    @Override
    public String getPropertyKeyName( long propertyKeyId ) throws PropertyKeyIdNotFoundException
    {
        beforeOperation();
        beforeReadOperation();

        String result = propertyOperations.getPropertyKeyName( propertyKeyId );

        afterReadOperation();
        afterOperation();
        return result;
    }

    @Override
    public Object getNodePropertyValue( long nodeId, long propertyKeyId ) throws PropertyKeyIdNotFoundException,
            PropertyNotFoundException, EntityNotFoundException
    {
        beforeOperation();
        beforeReadOperation();

        Object result = propertyOperations.getNodePropertyValue( nodeId, propertyKeyId );

        afterReadOperation();
        afterOperation();
        return result;
    }


    @Override
    public boolean nodeHasProperty(long nodeId, long propertyKeyId )
            throws PropertyKeyIdNotFoundException, EntityNotFoundException
    {
        beforeOperation();
        beforeReadOperation();

        boolean result = propertyOperations.nodeHasProperty(nodeId, propertyKeyId );

        afterReadOperation();
        afterOperation();
        return result;
    }

    @Override
    public Iterator<Long> listNodePropertyKeys( long nodeId )
    {
        beforeOperation();
        beforeReadOperation();

        Iterator<Long> result = propertyOperations.listNodePropertyKeys( nodeId );

        afterReadOperation();
        afterOperation();
        return result;
    }

    @Override
    public Iterator<Long> listRelationshipPropertyKeys( long relationshipId )
    {
        beforeOperation();
        beforeReadOperation();

        Iterator<Long> result = propertyOperations.listRelationshipPropertyKeys( relationshipId );

        afterReadOperation();
        afterOperation();
        return result;
    }

    @Override
    public IndexRule getIndexRule( long labelId, long propertyKey ) throws SchemaRuleNotFoundException
    {
        beforeOperation();
        beforeReadOperation();

        IndexRule result = schemaOperations.getIndexRule( labelId, propertyKey );

        afterReadOperation();
        afterOperation();
        return result;
    }

    @Override
    public IndexDescriptor getIndexDescriptor( long indexId ) throws IndexNotFoundKernelException
    {
        beforeOperation();
        beforeReadOperation();

        IndexDescriptor result = schemaOperations.getIndexDescriptor( indexId );

        afterReadOperation();
        afterOperation();
        return result;
    }

    @Override
    public Iterator<IndexRule> getIndexRules( long labelId )
    {
        beforeOperation();
        beforeReadOperation();

        Iterator<IndexRule> result = schemaOperations.getIndexRules( labelId );

        afterReadOperation();
        afterOperation();
        return result;
    }

    @Override
    public Iterator<IndexRule> getIndexRules()
    {
        beforeOperation();
        beforeReadOperation();

        Iterator<IndexRule> result = schemaOperations.getIndexRules();

        afterReadOperation();
        afterOperation();
        return result;
    }

    @Override
    public InternalIndexState getIndexState( IndexRule indexRule ) throws IndexNotFoundKernelException
    {
        beforeOperation();
        beforeReadOperation();

        InternalIndexState result = schemaOperations.getIndexState( indexRule );

        afterReadOperation();
        afterOperation();
        return result;
    }

    @Override
    public <K> boolean schemaStateContains( K key )
    {
        beforeOperation();
        beforeReadOperation();

        boolean result = schemaOperations.schemaStateContains( key );

        afterReadOperation();
        afterOperation();
        return result;
    }

    //
    // WRITE OPERATIONS
    //

    @Override
    public long getOrCreateLabelId( String label ) throws ConstraintViolationKernelException
    {
        beforeOperation();
        beforeWriteOperation();

        long result = labelOperations.getOrCreateLabelId( label );

        afterWriteOperation();
        afterOperation();
        return result;
    }

    @Override
    public boolean addLabelToNode( long labelId, long nodeId ) throws EntityNotFoundException
    {
        beforeOperation();
        beforeWriteOperation();

        boolean result = labelOperations.addLabelToNode( labelId, nodeId );

        afterWriteOperation();
        afterOperation();
        return result;
    }

    @Override
    public boolean removeLabelFromNode( long labelId, long nodeId ) throws EntityNotFoundException
    {
        beforeOperation();
        beforeWriteOperation();

        boolean result = labelOperations.removeLabelFromNode( labelId, nodeId );

        afterWriteOperation();
        afterOperation();
        return result;
    }

    @Override
    public long getOrCreatePropertyKeyId( String propertyKey ) throws ConstraintViolationKernelException
    {
        beforeOperation();
        beforeWriteOperation();

        long result = propertyOperations.getOrCreatePropertyKeyId( propertyKey );

        afterWriteOperation();
        afterOperation();
        return result;
    }

    @Override
    public IndexRule addIndexRule( long labelId, long propertyKey ) throws ConstraintViolationKernelException
    {
        beforeOperation();
        beforeWriteOperation();

        IndexRule result = schemaOperations.addIndexRule( labelId, propertyKey );

        afterWriteOperation();
        afterOperation();
        return result;
    }

    @Override
    public void dropIndexRule( IndexRule indexRule ) throws ConstraintViolationKernelException
    {
        beforeOperation();
        beforeWriteOperation();

        schemaOperations.dropIndexRule( indexRule );

        afterWriteOperation();
        afterOperation();
    }

    @Override
    public <K, V> V getOrCreateFromSchemaState( K key, Function<K, V> creator )
    {
        beforeOperation();

        V result = schemaOperations.getOrCreateFromSchemaState( key, creator );

        afterOperation();
        return result;
    }

    @Override
    public void nodeSetPropertyValue( long nodeId, long propertyKeyId, Object value )
            throws PropertyKeyIdNotFoundException, EntityNotFoundException
    {
        beforeOperation();
        beforeWriteOperation();

        propertyOperations.nodeSetPropertyValue( nodeId, propertyKeyId, value );

        afterWriteOperation();
        afterOperation();
    }

    @Override
    public Object nodeRemoveProperty( long nodeId, long propertyKeyId )
            throws PropertyKeyIdNotFoundException, EntityNotFoundException
    {
        beforeOperation();
        beforeWriteOperation();

        Object result = propertyOperations.nodeRemoveProperty( nodeId, propertyKeyId );

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

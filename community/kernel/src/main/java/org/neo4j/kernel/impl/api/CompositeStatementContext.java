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
import org.neo4j.kernel.api.StatementContext;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.LabelNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.PropertyKeyIdNotFoundException;
import org.neo4j.kernel.api.exceptions.PropertyKeyNotFoundException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.schema.DropIndexFailureException;
import org.neo4j.kernel.api.exceptions.schema.SchemaKernelException;
import org.neo4j.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.operations.EntityOperations;
import org.neo4j.kernel.api.operations.KeyOperations;
import org.neo4j.kernel.api.operations.SchemaOperations;
import org.neo4j.kernel.api.operations.SchemaStateOperations;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.impl.api.index.IndexDescriptor;
import org.neo4j.kernel.impl.core.Token;

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
    public <K, V> V schemaStateGetOrCreate( K key, Function<K, V> creator )
    {
        beforeOperation();

        V result = schemaStateOperations.schemaStateGetOrCreate( key, creator );

        afterOperation();
        return result;
    }

    //
    // READ OPERATIONS
    //

    @Override
    public Iterator<Long> nodesGetForLabel( long labelId )
    {
        beforeOperation();
        beforeReadOperation();

        Iterator<Long> result = entityOperations.nodesGetForLabel( labelId );

        afterReadOperation();
        afterOperation();
        return result;
    }

    @Override
    public Iterator<Long> nodesGetFromIndexLookup( IndexDescriptor index, Object value )
            throws IndexNotFoundKernelException
    {
        beforeOperation();
        beforeReadOperation();

        Iterator<Long> result = entityOperations.nodesGetFromIndexLookup( index, value );

        afterReadOperation();
        afterOperation();
        return result;
    }

    @Override
    public long labelGetForName( String label ) throws LabelNotFoundKernelException
    {
        beforeOperation();
        beforeReadOperation();

        long result = keyOperations.labelGetForName( label );

        afterReadOperation();
        afterOperation();
        return result;
    }

    @Override
    public String labelGetName( long labelId ) throws LabelNotFoundKernelException
    {
        beforeOperation();
        beforeReadOperation();

        String result = keyOperations.labelGetName( labelId );

        afterReadOperation();
        afterOperation();
        return result;
    }

    @Override
    public boolean nodeHasLabel( long nodeId, long labelId ) throws EntityNotFoundException
    {
        beforeOperation();
        beforeReadOperation();

        boolean result = entityOperations.nodeHasLabel( nodeId, labelId );

        afterReadOperation();
        afterOperation();
        return result;
    }

    @Override
    public Iterator<Long> nodeGetLabels( long nodeId ) throws EntityNotFoundException
    {
        beforeOperation();
        beforeReadOperation();

        Iterator<Long> result = entityOperations.nodeGetLabels( nodeId );

        afterReadOperation();
        afterOperation();
        return result;
    }

    @Override
    public long propertyKeyGetForName( String propertyKey ) throws PropertyKeyNotFoundException
    {
        beforeOperation();
        beforeReadOperation();

        long result = keyOperations.propertyKeyGetForName( propertyKey );

        afterReadOperation();
        afterOperation();
        return result;
    }

    @Override
    public String propertyKeyGetName( long propertyKeyId ) throws PropertyKeyIdNotFoundException
    {
        beforeOperation();
        beforeReadOperation();

        String result = keyOperations.propertyKeyGetName( propertyKeyId );

        afterReadOperation();
        afterOperation();
        return result;
    }

    @Override
    public Property nodeGetProperty( long nodeId, long propertyKeyId )
            throws PropertyKeyIdNotFoundException, EntityNotFoundException
    {
        beforeOperation();
        beforeReadOperation();

        Property result = entityOperations.nodeGetProperty( nodeId, propertyKeyId );

        afterReadOperation();
        afterOperation();
        return result;
    }

    @Override
    public Property relationshipGetProperty( long relationshipId, long propertyKeyId )
            throws PropertyKeyIdNotFoundException, EntityNotFoundException
    {
        beforeOperation();
        beforeReadOperation();

        Property result = entityOperations.relationshipGetProperty( relationshipId, propertyKeyId );

        afterReadOperation();
        afterOperation();
        return result;
    }

    @Override
    public boolean nodeHasProperty( long nodeId, long propertyKeyId )
            throws PropertyKeyIdNotFoundException, EntityNotFoundException
    {
        beforeOperation();
        beforeReadOperation();

        boolean result = entityOperations.nodeHasProperty( nodeId, propertyKeyId );

        afterReadOperation();
        afterOperation();
        return result;
    }

    @Override
    public boolean relationshipHasProperty( long relationshipId, long propertyKeyId )
            throws PropertyKeyIdNotFoundException, EntityNotFoundException
    {
        beforeOperation();
        beforeReadOperation();

        boolean result = entityOperations.relationshipHasProperty( relationshipId, propertyKeyId );

        afterReadOperation();
        afterOperation();
        return result;
    }

    @Override
    public Iterator<Long> nodeGetPropertyKeys( long nodeId ) throws EntityNotFoundException
    {
        beforeOperation();
        beforeReadOperation();

        Iterator<Long> result = entityOperations.nodeGetPropertyKeys( nodeId );

        afterReadOperation();
        afterOperation();
        return result;
    }

    @Override
    public Iterator<Property> nodeGetAllProperties( long nodeId ) throws EntityNotFoundException
    {
        beforeOperation();
        beforeReadOperation();

        Iterator<Property> result = entityOperations.nodeGetAllProperties( nodeId );

        afterReadOperation();
        afterOperation();
        return result;
    }

    @Override
    public Iterator<Long> relationshipGetPropertyKeys( long relationshipId ) throws EntityNotFoundException
    {
        beforeOperation();
        beforeReadOperation();

        Iterator<Long> result = entityOperations.relationshipGetPropertyKeys( relationshipId );

        afterReadOperation();
        afterOperation();
        return result;
    }

    @Override
    public Iterator<Property> relationshipGetAllProperties( long relationshipId ) throws EntityNotFoundException
    {
        beforeOperation();
        beforeReadOperation();

        Iterator<Property> result = entityOperations.relationshipGetAllProperties( relationshipId );

        afterReadOperation();
        afterOperation();
        return result;
    }

    @Override
    public IndexDescriptor indexesGetForLabelAndPropertyKey( long labelId, long propertyKey )
            throws SchemaRuleNotFoundException
    {
        beforeOperation();
        beforeReadOperation();

        IndexDescriptor result = schemaOperations.indexesGetForLabelAndPropertyKey( labelId, propertyKey );

        afterReadOperation();
        afterOperation();
        return result;
    }

    @Override
    public Iterator<IndexDescriptor> indexesGetForLabel( long labelId )
    {
        beforeOperation();
        beforeReadOperation();

        Iterator<IndexDescriptor> result = schemaOperations.indexesGetForLabel( labelId );

        afterReadOperation();
        afterOperation();
        return result;
    }

    @Override
    public Iterator<IndexDescriptor> indexesGetAll()
    {
        beforeOperation();
        beforeReadOperation();

        Iterator<IndexDescriptor> result = schemaOperations.indexesGetAll();

        afterReadOperation();
        afterOperation();
        return result;
    }

    @Override
    public Iterator<IndexDescriptor> uniqueIndexesGetForLabel( long labelId )
    {
        beforeOperation();
        beforeReadOperation();

        Iterator<IndexDescriptor> result = schemaOperations.uniqueIndexesGetForLabel( labelId );

        afterReadOperation();
        afterOperation();
        return result;
    }

    @Override
    public Iterator<IndexDescriptor> uniqueIndexesGetAll()
    {
        beforeOperation();
        beforeReadOperation();

        Iterator<IndexDescriptor> result = schemaOperations.uniqueIndexesGetAll();

        afterReadOperation();
        afterOperation();
        return result;
    }

    @Override
    public InternalIndexState indexGetState( IndexDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        beforeOperation();
        beforeReadOperation();

        InternalIndexState result = schemaOperations.indexGetState( descriptor );

        afterReadOperation();
        afterOperation();
        return result;
    }

    @Override
    public Iterator<UniquenessConstraint> constraintsGetForLabelAndPropertyKey( long labelId, long propertyKeyId )
    {
        beforeOperation();
        beforeReadOperation();

        Iterator<UniquenessConstraint> result = schemaOperations.constraintsGetForLabelAndPropertyKey( labelId,
                                                                                                       propertyKeyId );

        afterReadOperation();
        afterOperation();
        return result;
    }

    @Override
    public Iterator<UniquenessConstraint> constraintsGetForLabel( long labelId )
    {
        beforeOperation();
        beforeReadOperation();

        Iterator<UniquenessConstraint> result = schemaOperations.constraintsGetForLabel( labelId );

        afterReadOperation();
        afterOperation();
        return result;
    }

    @Override
    public Long indexGetOwningUniquenessConstraintId( IndexDescriptor index ) throws SchemaRuleNotFoundException
    {
        beforeOperation();
        beforeReadOperation();

        Long result = schemaOperations.indexGetOwningUniquenessConstraintId( index );

        afterReadOperation();
        afterOperation();
        return result;
    }

    @Override
    public long indexGetCommittedId( IndexDescriptor index ) throws SchemaRuleNotFoundException
    {
        beforeOperation();
        beforeReadOperation();

        long result = schemaOperations.indexGetCommittedId( index );

        afterReadOperation();
        afterOperation();
        return result;
    }

    @Override
    public Iterator<UniquenessConstraint> constraintsGetAll()
    {
        beforeOperation();
        beforeReadOperation();

        Iterator<UniquenessConstraint> result = schemaOperations.constraintsGetAll();

        afterReadOperation();
        afterOperation();
        return result;
    }

    @Override
    public Iterator<Token> labelsGetAllTokens()
    {
        beforeOperation();
        beforeReadOperation();

        Iterator<Token> result = keyOperations.labelsGetAllTokens();

        afterReadOperation();
        afterOperation();
        return result;
    }

    //
    // WRITE OPERATIONS
    //

    @Override
    public long labelGetOrCreateForName( String label ) throws SchemaKernelException
    {
        beforeOperation();
        beforeWriteOperation();

        long result = keyOperations.labelGetOrCreateForName( label );

        afterWriteOperation();
        afterOperation();
        return result;
    }

    @Override
    public boolean nodeAddLabel( long nodeId, long labelId ) throws EntityNotFoundException
    {
        beforeOperation();
        beforeWriteOperation();

        boolean result = entityOperations.nodeAddLabel( nodeId, labelId );

        afterWriteOperation();
        afterOperation();
        return result;
    }

    @Override
    public boolean nodeRemoveLabel( long nodeId, long labelId ) throws EntityNotFoundException
    {
        beforeOperation();
        beforeWriteOperation();

        boolean result = entityOperations.nodeRemoveLabel( nodeId, labelId );

        afterWriteOperation();
        afterOperation();
        return result;
    }

    @Override
    public long propertyKeyGetOrCreateForName( String propertyKey ) throws SchemaKernelException
    {
        beforeOperation();
        beforeWriteOperation();

        long result = keyOperations.propertyKeyGetOrCreateForName( propertyKey );

        afterWriteOperation();
        afterOperation();
        return result;
    }

    @Override
    public IndexDescriptor indexCreate( long labelId, long propertyKey ) throws SchemaKernelException
    {
        beforeOperation();
        beforeWriteOperation();

        IndexDescriptor result = schemaOperations.indexCreate( labelId, propertyKey );

        afterWriteOperation();
        afterOperation();
        return result;
    }

    @Override
    public IndexDescriptor uniqueIndexCreate( long labelId, long propertyKey ) throws SchemaKernelException
    {
        beforeOperation();
        beforeWriteOperation();

        IndexDescriptor result = schemaOperations.uniqueIndexCreate( labelId, propertyKey );

        afterWriteOperation();
        afterOperation();
        return result;
    }

    @Override
    public UniquenessConstraint uniquenessConstraintCreate( long labelId, long propertyKeyId )
            throws SchemaKernelException
    {
        beforeOperation();
        beforeWriteOperation();

        UniquenessConstraint result = schemaOperations.uniquenessConstraintCreate( labelId, propertyKeyId );

        afterWriteOperation();
        afterOperation();
        return result;
    }

    @Override
    public void constraintDrop( UniquenessConstraint constraint )
    {
        beforeOperation();
        beforeWriteOperation();

        schemaOperations.constraintDrop( constraint );

        afterWriteOperation();
        afterOperation();
    }

    @Override
    public void indexDrop( IndexDescriptor descriptor ) throws DropIndexFailureException
    {
        beforeOperation();
        beforeWriteOperation();

        schemaOperations.indexDrop( descriptor );

        afterWriteOperation();
        afterOperation();
    }

    @Override
    public void uniqueIndexDrop( IndexDescriptor descriptor ) throws DropIndexFailureException
    {
        beforeOperation();
        beforeWriteOperation();

        schemaOperations.uniqueIndexDrop( descriptor );

        afterWriteOperation();
        afterOperation();
    }

    @Override
    public Property nodeSetProperty( long nodeId, Property property )
            throws PropertyKeyIdNotFoundException, EntityNotFoundException
    {
        beforeOperation();
        beforeWriteOperation();

        Property result = entityOperations.nodeSetProperty( nodeId, property );

        afterWriteOperation();
        afterOperation();

        return result;
    }

    @Override
    public Property relationshipSetProperty( long relationshipId, Property property )
            throws PropertyKeyIdNotFoundException, EntityNotFoundException
    {
        beforeOperation();
        beforeWriteOperation();

        Property result = entityOperations.relationshipSetProperty( relationshipId, property );

        afterWriteOperation();
        afterOperation();

        return result;
    }

    @Override
    public Property nodeRemoveProperty( long nodeId, long propertyKeyId )
            throws PropertyKeyIdNotFoundException, EntityNotFoundException
    {
        beforeOperation();
        beforeWriteOperation();

        Property result = entityOperations.nodeRemoveProperty( nodeId, propertyKeyId );

        afterWriteOperation();
        afterOperation();

        return result;
    }

    @Override
    public Property relationshipRemoveProperty( long relationshipId, long propertyKeyId )
            throws PropertyKeyIdNotFoundException, EntityNotFoundException
    {
        beforeOperation();
        beforeWriteOperation();

        Property result = entityOperations.relationshipRemoveProperty( relationshipId, propertyKeyId );

        afterWriteOperation();
        afterOperation();

        return result;
    }

    @Override
    public void nodeDelete( long nodeId )
    {
        beforeOperation();
        beforeWriteOperation();

        entityOperations.nodeDelete( nodeId );

        afterWriteOperation();
        afterOperation();
    }


    @Override
    public void relationshipDelete( long relationshipId )
    {
        beforeOperation();
        beforeWriteOperation();

        entityOperations.relationshipDelete( relationshipId );

        afterWriteOperation();
        afterOperation();
    }
}

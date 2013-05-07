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
import org.neo4j.kernel.impl.api.index.IndexDescriptor;
import org.neo4j.kernel.impl.core.LabelToken;

public enum UnsupportiveStatementContext implements StatementContext
{
    INSTANCE;

    public static StatementContext instance()
    {
        return INSTANCE;
    }

    @Override
    public void close()
    {
        throw unsupported();
    }

    @Override
    public Iterator<Long> getNodesWithLabel( long labelId )
    {
        throw unsupported();
    }

    @Override
    public Iterator<Long> exactIndexLookup( IndexDescriptor index, Object value ) throws IndexNotFoundKernelException
    {
        throw unsupported();
    }

    @Override
    public void deleteNode( long nodeId )
    {
        throw unsupported();
    }

    @Override
    public long getOrCreateLabelId( String label ) throws DataIntegrityKernelException
    {
        throw unsupported();
    }

    @Override
    public long getLabelId( String label ) throws LabelNotFoundKernelException
    {
        throw unsupported();
    }

    @Override
    public String getLabelName( long labelId ) throws LabelNotFoundKernelException
    {
        throw unsupported();
    }

    @Override
    public boolean addLabelToNode( long labelId, long nodeId ) throws EntityNotFoundException
    {
        throw unsupported();
    }

    @Override
    public boolean isLabelSetOnNode( long labelId, long nodeId ) throws EntityNotFoundException
    {
        throw unsupported();
    }

    @Override
    public Iterator<Long> getLabelsForNode( long nodeId ) throws EntityNotFoundException
    {
        throw unsupported();
    }

    @Override
    public boolean removeLabelFromNode( long labelId, long nodeId ) throws EntityNotFoundException
    {
        throw unsupported();
    }

    @Override
    public Iterator<LabelToken> listLabels()
    {
        throw unsupported();
    }

    @Override
    public long getOrCreatePropertyKeyId( String propertyKey ) throws DataIntegrityKernelException
    {
        throw unsupported();
    }

    @Override
    public long getPropertyKeyId( String propertyKey ) throws PropertyKeyNotFoundException
    {
        throw unsupported();
    }

    @Override
    public String getPropertyKeyName( long propertyId ) throws PropertyKeyIdNotFoundException
    {
        throw unsupported();
    }

    @Override
    public Object getNodePropertyValue( long nodeId, long propertyId ) throws PropertyKeyIdNotFoundException,
                                                                              PropertyNotFoundException,
                                                                              EntityNotFoundException
    {
        throw unsupported();
    }

    @Override
    public boolean nodeHasProperty( long nodeId, long propertyId ) throws PropertyKeyIdNotFoundException,
                                                                          EntityNotFoundException
    {
        throw unsupported();
    }

    @Override
    public void nodeSetPropertyValue( long nodeId, long propertyId,
                                      Object value ) throws PropertyKeyIdNotFoundException, EntityNotFoundException
    {
        throw unsupported();
    }

    @Override
    public Object nodeRemoveProperty( long nodeId, long propertyId ) throws PropertyKeyIdNotFoundException,
                                                                            EntityNotFoundException
    {
        throw unsupported();
    }

    @Override
    public Iterator<Long> listNodePropertyKeys( long nodeId )
    {
        throw unsupported();
    }

    @Override
    public Iterator<Long> listRelationshipPropertyKeys( long relationshipId )
    {
        throw unsupported();
    }

    @Override
    public IndexDescriptor addIndex( long labelId, long propertyKey ) throws
                                                                      DataIntegrityKernelException
    {
        throw unsupported();
    }

    @Override
    public IndexDescriptor addConstraintIndex( long labelId, long propertyKey )
            throws DataIntegrityKernelException
    {
        throw unsupported();
    }

    @Override
    public IndexDescriptor getIndex( long labelId, long propertyKey ) throws SchemaRuleNotFoundException
    {
        throw unsupported();
    }

    @Override
    public Iterator<IndexDescriptor> getIndexes( long labelId )
    {
        throw unsupported();
    }

    @Override
    public Iterator<IndexDescriptor> getIndexes()
    {
        throw unsupported();
    }

    @Override
    public Iterator<IndexDescriptor> getConstraintIndexes( long labelId )
    {
        throw unsupported();
    }

    @Override
    public Iterator<IndexDescriptor> getConstraintIndexes()
    {
        throw unsupported();
    }

    @Override
    public InternalIndexState getIndexState( IndexDescriptor indexRule ) throws IndexNotFoundKernelException
    {
        throw unsupported();
    }

    @Override
    public void dropIndex( IndexDescriptor descriptor ) throws DataIntegrityKernelException
    {
        throw unsupported();
    }

    @Override
    public void dropConstraintIndex( IndexDescriptor descriptor ) throws DataIntegrityKernelException
    {
        throw unsupported();
    }

    @Override
    public <K, V> V getOrCreateFromSchemaState( K key, Function<K, V> creator )
    {
        throw unsupported();
    }

    @Override
    public <K> boolean schemaStateContains( K key )
    {
        throw unsupported();
    }

    @Override
    public UniquenessConstraint addUniquenessConstraint( long labelId, long propertyKeyId )
    {
        throw unsupported();
    }

    @Override
    public Iterator<UniquenessConstraint> getConstraints( long labelId, long propertyKeyId )
    {
        throw unsupported();
    }

    @Override
    public Iterator<UniquenessConstraint> getConstraints( long labelId )
    {
        throw unsupported();
    }

    @Override
    public Long getOwningConstraint( IndexDescriptor index )
    {
        throw unsupported();
    }

    @Override
    public long getCommittedIndexId( IndexDescriptor index ) throws SchemaRuleNotFoundException
    {
        throw unsupported();
    }

    @Override
    public Iterator<UniquenessConstraint> getConstraints()
    {
        throw unsupported();
    }

    @Override
    public void dropConstraint( UniquenessConstraint constraint )
    {
        throw unsupported();
    }

    private UnsupportedOperationException unsupported()
    {
        return new UnsupportedOperationException( "This operation is not implemented." );
    }
}

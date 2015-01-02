/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.api.store;

import java.util.Iterator;

import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.LabelNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.PropertyKeyIdNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.RelationshipTypeIdNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.schema.IndexBrokenKernelException;
import org.neo4j.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.exceptions.schema.TooManyLabelsException;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.impl.api.KernelStatement;
import org.neo4j.kernel.impl.core.Token;
import org.neo4j.kernel.impl.nioneo.store.IndexRule;
import org.neo4j.kernel.impl.nioneo.store.SchemaStorage;
import org.neo4j.kernel.impl.util.PrimitiveIntIterator;
import org.neo4j.kernel.impl.util.PrimitiveLongIterator;
import org.neo4j.kernel.impl.util.PrimitiveLongResourceIterator;

/**
 * Abstraction for reading committed data.
 */
public interface StoreReadLayer
{
    boolean nodeHasLabel( KernelStatement state, long nodeId, int labelId ) throws EntityNotFoundException;

    PrimitiveIntIterator nodeGetLabels( KernelStatement state, long nodeId ) throws EntityNotFoundException;

    Iterator<IndexDescriptor> indexesGetForLabel( KernelStatement state, int labelId );

    Iterator<IndexDescriptor> indexesGetAll( KernelStatement state );

    Iterator<IndexDescriptor> uniqueIndexesGetForLabel( KernelStatement state, int labelId );

    Iterator<IndexDescriptor> uniqueIndexesGetAll( KernelStatement state );

    Long indexGetOwningUniquenessConstraintId( KernelStatement state, IndexDescriptor index )
            throws SchemaRuleNotFoundException;

    long indexGetCommittedId( KernelStatement state, IndexDescriptor index, SchemaStorage.IndexRuleKind kind ) throws SchemaRuleNotFoundException;

    IndexRule indexRule( IndexDescriptor index, SchemaStorage.IndexRuleKind kind );

    PrimitiveLongIterator nodeGetPropertyKeys( KernelStatement state, long nodeId ) throws EntityNotFoundException;

    Property nodeGetProperty( KernelStatement state, long nodeId, int propertyKeyId ) throws EntityNotFoundException;

    Iterator<DefinedProperty> nodeGetAllProperties( KernelStatement state, long nodeId ) throws EntityNotFoundException;

    PrimitiveLongIterator relationshipGetPropertyKeys( KernelStatement state, long relationshipId )
                    throws EntityNotFoundException;

    Property relationshipGetProperty( KernelStatement state, long relationshipId, int propertyKeyId )
                            throws EntityNotFoundException;

    Iterator<DefinedProperty> relationshipGetAllProperties( KernelStatement state, long nodeId )
                                    throws EntityNotFoundException;

    PrimitiveLongIterator graphGetPropertyKeys( KernelStatement state );

    Property graphGetProperty( KernelStatement state, int propertyKeyId );

    Iterator<DefinedProperty> graphGetAllProperties( KernelStatement state );

    Iterator<UniquenessConstraint> constraintsGetForLabelAndPropertyKey(
            KernelStatement state, int labelId, int propertyKeyId );

    Iterator<UniquenessConstraint> constraintsGetForLabel( KernelStatement state, int labelId );

    Iterator<UniquenessConstraint> constraintsGetAll( KernelStatement state );

    PrimitiveLongResourceIterator nodeGetUniqueFromIndexLookup( KernelStatement state, IndexDescriptor index,
                                                        Object value )
            throws IndexNotFoundKernelException, IndexBrokenKernelException;

    PrimitiveLongIterator nodesGetForLabel( KernelStatement state, int labelId );

    PrimitiveLongIterator nodesGetFromIndexLookup( KernelStatement state, IndexDescriptor index, Object value )
                                                            throws IndexNotFoundKernelException;

    IndexDescriptor indexesGetForLabelAndPropertyKey( KernelStatement state, int labelId, int propertyKey )
                                                                    throws SchemaRuleNotFoundException;

    InternalIndexState indexGetState( KernelStatement state, IndexDescriptor descriptor )
                                                                            throws IndexNotFoundKernelException;

    String indexGetFailure( Statement state, IndexDescriptor descriptor ) throws IndexNotFoundKernelException;

    int labelGetForName( String labelName );

    String labelGetName( int labelId ) throws LabelNotFoundKernelException;

    int propertyKeyGetForName( String propertyKeyName );

    int propertyKeyGetOrCreateForName( String propertyKeyName );

    String propertyKeyGetName( int propertyKeyId ) throws PropertyKeyIdNotFoundKernelException;

    Iterator<Token> propertyKeyGetAllTokens();

    Iterator<Token> labelsGetAllTokens();

    int relationshipTypeGetForName( String relationshipTypeName );

    String relationshipTypeGetName( int relationshipTypeId ) throws RelationshipTypeIdNotFoundKernelException;

    int labelGetOrCreateForName( String labelName ) throws TooManyLabelsException;

    int relationshipTypeGetOrCreateForName( String relationshipTypeName );
}

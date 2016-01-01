/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphdb.Direction;
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
import org.neo4j.kernel.impl.util.PrimitiveLongResourceIterator;

/**
 * Abstraction for reading committed data.
 */
public interface StoreReadLayer
{
    boolean nodeHasLabel( long nodeId, int labelId ) throws EntityNotFoundException;

    boolean nodeExists( long nodeId );

    PrimitiveIntIterator nodeGetLabels( long nodeId ) throws EntityNotFoundException;

    PrimitiveLongIterator nodeListRelationships( KernelStatement state, long nodeId, Direction direction) throws EntityNotFoundException;

    PrimitiveLongIterator nodeListRelationships( long nodeId, Direction direction, int[] relTypes ) throws EntityNotFoundException;

    int nodeGetDegree( long nodeId, Direction direction ) throws EntityNotFoundException;
    int nodeGetDegree( long nodeId, Direction direction, int relType ) throws EntityNotFoundException;

    PrimitiveIntIterator nodeGetRelationshipTypes( long nodeId ) throws EntityNotFoundException;

    Iterator<IndexDescriptor> indexesGetForLabel( int labelId );

    Iterator<IndexDescriptor> indexesGetAll();

    Iterator<IndexDescriptor> uniqueIndexesGetForLabel( int labelId );

    Iterator<IndexDescriptor> uniqueIndexesGetAll();

    Long indexGetOwningUniquenessConstraintId( IndexDescriptor index )
            throws SchemaRuleNotFoundException;

    long indexGetCommittedId( IndexDescriptor index, SchemaStorage.IndexRuleKind kind ) throws SchemaRuleNotFoundException;

    IndexRule indexRule( IndexDescriptor index, SchemaStorage.IndexRuleKind kind );

    PrimitiveLongIterator nodeGetPropertyKeys( long nodeId ) throws EntityNotFoundException;

    Property nodeGetProperty( long nodeId, int propertyKeyId ) throws EntityNotFoundException;

    Iterator<DefinedProperty> nodeGetAllProperties( long nodeId ) throws EntityNotFoundException;

    boolean relationshipExists( long relationshipId );

    PrimitiveLongIterator relationshipGetPropertyKeys( long relationshipId )
                    throws EntityNotFoundException;

    Property relationshipGetProperty( long relationshipId, int propertyKeyId )
                            throws EntityNotFoundException;

    Iterator<DefinedProperty> relationshipGetAllProperties( long nodeId )
                                    throws EntityNotFoundException;

    PrimitiveLongIterator graphGetPropertyKeys( KernelStatement state );

    Property graphGetProperty( int propertyKeyId );

    Iterator<DefinedProperty> graphGetAllProperties();

    Iterator<UniquenessConstraint> constraintsGetForLabelAndPropertyKey(
            int labelId, int propertyKeyId );

    Iterator<UniquenessConstraint> constraintsGetForLabel( int labelId );

    Iterator<UniquenessConstraint> constraintsGetAll();

    PrimitiveLongResourceIterator nodeGetUniqueFromIndexLookup( KernelStatement state, IndexDescriptor index,
                                                        Object value )
            throws IndexNotFoundKernelException, IndexBrokenKernelException;

    PrimitiveLongIterator nodesGetForLabel( KernelStatement state, int labelId );

    PrimitiveLongResourceIterator nodesGetFromIndexLookup( KernelStatement state, IndexDescriptor index, Object value )
            throws IndexNotFoundKernelException;

    IndexDescriptor indexesGetForLabelAndPropertyKey( int labelId, int propertyKey );

    InternalIndexState indexGetState( IndexDescriptor descriptor ) throws IndexNotFoundKernelException;

    String indexGetFailure( IndexDescriptor descriptor ) throws IndexNotFoundKernelException;

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

    void visit( long relationshipId, RelationshipVisitor relationshipVisitor ) throws EntityNotFoundException;

    long highestNodeIdInUse();

    public interface RelationshipVisitor
    {
        void visit( long relId, long startNode, long endNode, int type );
    }
}

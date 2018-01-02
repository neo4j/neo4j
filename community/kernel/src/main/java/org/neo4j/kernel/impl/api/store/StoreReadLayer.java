/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
import org.neo4j.kernel.api.constraints.NodePropertyConstraint;
import org.neo4j.kernel.api.constraints.PropertyConstraint;
import org.neo4j.kernel.api.constraints.RelationshipPropertyConstraint;
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
import org.neo4j.kernel.api.procedures.ProcedureDescriptor;
import org.neo4j.kernel.api.procedures.ProcedureSignature.ProcedureName;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.impl.api.KernelStatement;
import org.neo4j.kernel.impl.api.RelationshipVisitor;
import org.neo4j.kernel.impl.core.Token;
import org.neo4j.kernel.impl.store.SchemaStorage;
import org.neo4j.kernel.impl.store.record.IndexRule;
import org.neo4j.kernel.impl.util.PrimitiveLongResourceIterator;

/**
 * Abstraction for reading committed data.
 */
public interface StoreReadLayer
{
    // Cursor
    StoreStatement acquireStatement();

    Iterator<IndexDescriptor> indexesGetForLabel( int labelId );

    Iterator<IndexDescriptor> indexesGetAll();

    Iterator<IndexDescriptor> uniqueIndexesGetForLabel( int labelId );

    Iterator<IndexDescriptor> uniqueIndexesGetAll();

    Long indexGetOwningUniquenessConstraintId( IndexDescriptor index )
            throws SchemaRuleNotFoundException;

    long indexGetCommittedId( IndexDescriptor index, SchemaStorage.IndexRuleKind kind )
            throws SchemaRuleNotFoundException;

    IndexRule indexRule( IndexDescriptor index, SchemaStorage.IndexRuleKind kind );

    PrimitiveIntIterator graphGetPropertyKeys( KernelStatement state );

    Object graphGetProperty( int propertyKeyId );

    Iterator<DefinedProperty> graphGetAllProperties();

    Iterator<NodePropertyConstraint> constraintsGetForLabelAndPropertyKey( int labelId, int propertyKeyId );

    Iterator<NodePropertyConstraint> constraintsGetForLabel( int labelId );

    Iterator<RelationshipPropertyConstraint> constraintsGetForRelationshipTypeAndPropertyKey( int typeId,
            int propertyKeyId );

    Iterator<RelationshipPropertyConstraint> constraintsGetForRelationshipType( int typeId );

    Iterator<PropertyConstraint> constraintsGetAll();

    PrimitiveLongResourceIterator nodeGetFromUniqueIndexSeek( KernelStatement state, IndexDescriptor index,
            Object value )
            throws IndexNotFoundKernelException, IndexBrokenKernelException;

    PrimitiveLongIterator nodesGetForLabel( KernelStatement state, int labelId );

    PrimitiveLongIterator nodesGetFromIndexSeek( KernelStatement state, IndexDescriptor index, Object value )
            throws IndexNotFoundKernelException;

    PrimitiveLongIterator nodesGetFromInclusiveNumericIndexRangeSeek( KernelStatement statement, IndexDescriptor index, Number lower, Number upper )
            throws IndexNotFoundKernelException;

    PrimitiveLongIterator nodesGetFromIndexRangeSeekByString( KernelStatement statement, IndexDescriptor index, String lower, boolean includeLower, String upper, boolean includeUpper )
            throws IndexNotFoundKernelException;

    PrimitiveLongIterator nodesGetFromIndexRangeSeekByPrefix( KernelStatement state, IndexDescriptor index, String prefix )
            throws IndexNotFoundKernelException;

    PrimitiveLongIterator nodesGetFromIndexScan( KernelStatement state, IndexDescriptor index )
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

    <EXCEPTION extends Exception> void relationshipVisit( long relationshipId,
            RelationshipVisitor<EXCEPTION> relationshipVisitor ) throws EntityNotFoundException, EXCEPTION;

    long highestNodeIdInUse();

    PrimitiveLongIterator nodesGetAll();

    RelationshipIterator relationshipsGetAll();

    /**
     * Reserves a node id for future use.
     */
    long reserveNode();

    /**
     * Reserves a relationship id for future use.
     */
    long reserveRelationship();

    void releaseNode( long id );

    void releaseRelationship( long id );

    long countsForNode( int labelId );

    long countsForRelationship( int startLabelId, int typeId, int endLabelId );

    long indexSize( IndexDescriptor descriptor ) throws IndexNotFoundKernelException;

    double indexUniqueValuesPercentage( IndexDescriptor descriptor ) throws IndexNotFoundKernelException;

    /** Return descriptors for all committed stored procedures */
    Iterator<ProcedureDescriptor> proceduresGetAll();

    /** Return the description of the specified procedure */
    ProcedureDescriptor procedureGet( ProcedureName name );
}

/*
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
package org.neo4j.kernel.impl.api;

import java.util.Iterator;
import java.util.Map;

import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.cursor.Cursor;
import org.neo4j.function.Function;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.DataWriteOperations;
import org.neo4j.kernel.api.EntityType;
import org.neo4j.kernel.api.LegacyIndexHits;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.SchemaWriteOperations;
import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.kernel.api.constraints.MandatoryNodePropertyConstraint;
import org.neo4j.kernel.api.constraints.MandatoryRelationshipPropertyConstraint;
import org.neo4j.kernel.api.constraints.NodePropertyConstraint;
import org.neo4j.kernel.api.constraints.PropertyConstraint;
import org.neo4j.kernel.api.constraints.RelationshipPropertyConstraint;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.cursor.NodeItem;
import org.neo4j.kernel.api.cursor.RelationshipItem;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.LabelNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.PropertyKeyIdNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.RelationshipTypeIdNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.legacyindex.LegacyIndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.schema.AlreadyConstrainedException;
import org.neo4j.kernel.api.exceptions.schema.AlreadyIndexedException;
import org.neo4j.kernel.api.exceptions.schema.ConstraintValidationKernelException;
import org.neo4j.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.kernel.api.exceptions.schema.DropConstraintFailureException;
import org.neo4j.kernel.api.exceptions.schema.DropIndexFailureException;
import org.neo4j.kernel.api.exceptions.schema.IllegalTokenNameException;
import org.neo4j.kernel.api.exceptions.schema.IndexBrokenKernelException;
import org.neo4j.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.exceptions.schema.TooManyLabelsException;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.impl.api.operations.CountsOperations;
import org.neo4j.kernel.impl.api.operations.EntityReadOperations;
import org.neo4j.kernel.impl.api.operations.EntityWriteOperations;
import org.neo4j.kernel.impl.api.operations.KeyReadOperations;
import org.neo4j.kernel.impl.api.operations.KeyWriteOperations;
import org.neo4j.kernel.impl.api.operations.LegacyIndexReadOperations;
import org.neo4j.kernel.impl.api.operations.LegacyIndexWriteOperations;
import org.neo4j.kernel.impl.api.operations.LockOperations;
import org.neo4j.kernel.impl.api.operations.SchemaReadOperations;
import org.neo4j.kernel.impl.api.operations.SchemaStateOperations;
import org.neo4j.kernel.impl.api.store.RelationshipIterator;
import org.neo4j.kernel.impl.core.Token;
import org.neo4j.kernel.impl.locking.Locks;

public class OperationsFacade implements ReadOperations, DataWriteOperations, SchemaWriteOperations
{
    final KernelStatement statement;
    private final StatementOperationParts operations;

    OperationsFacade( KernelStatement statement, StatementOperationParts operations )
    {
        this.statement = statement;
        this.operations = operations;
    }

    final KeyReadOperations tokenRead()
    {
        return operations.keyReadOperations();
    }

    final KeyWriteOperations tokenWrite()
    {
        return operations.keyWriteOperations();
    }

    final EntityReadOperations dataRead()
    {
        return operations.entityReadOperations();
    }

    final EntityWriteOperations dataWrite()
    {
        return operations.entityWriteOperations();
    }

    final LegacyIndexWriteOperations legacyIndexWrite()
    {
        return operations.legacyIndexWriteOperations();
    }

    final LegacyIndexReadOperations legacyIndexRead()
    {
        return operations.legacyIndexReadOperations();
    }

    final SchemaReadOperations schemaRead()
    {
        return operations.schemaReadOperations();
    }

    final org.neo4j.kernel.impl.api.operations.SchemaWriteOperations schemaWrite()
    {
        return operations.schemaWriteOperations();
    }

    final SchemaStateOperations schemaState()
    {
        return operations.schemaStateOperations();
    }

    final LockOperations locking()
    {
        return operations.locking();
    }

    final CountsOperations counting()
    {
        return operations.counting();
    }

    // <DataRead>

    @Override
    public PrimitiveLongIterator nodesGetAll()
    {
        statement.assertOpen();
        return dataRead().nodesGetAll( statement );
    }

    @Override
    public PrimitiveLongIterator relationshipsGetAll()
    {
        statement.assertOpen();
        return dataRead().relationshipsGetAll( statement );
    }

    @Override
    public PrimitiveLongIterator nodesGetForLabel( int labelId )
    {
        statement.assertOpen();
        if ( labelId == StatementConstants.NO_SUCH_LABEL )
        {
            return PrimitiveLongCollections.emptyIterator();
        }
        return dataRead().nodesGetForLabel( statement, labelId );
    }

    @Override
    public PrimitiveLongIterator nodesGetFromIndexSeek( IndexDescriptor index, Object value )
            throws IndexNotFoundKernelException
    {
        statement.assertOpen();
        return dataRead().nodesGetFromIndexSeek( statement, index, value );
    }

    @Override
    public PrimitiveLongIterator nodesGetFromIndexRangeSeekByNumber( IndexDescriptor index,
            Number lower,
            boolean includeLower,
            Number upper,
            boolean includeUpper )
            throws IndexNotFoundKernelException
    {
        statement.assertOpen();
        return dataRead().nodesGetFromIndexRangeSeekByNumber( statement, index, lower, includeLower, upper,
                includeUpper );
    }

    @Override
    public PrimitiveLongIterator nodesGetFromIndexRangeSeekByString( IndexDescriptor index,
            String lower,
            boolean includeLower,
            String upper,
            boolean includeUpper )
            throws IndexNotFoundKernelException
    {
        statement.assertOpen();
        return dataRead().nodesGetFromIndexRangeSeekByString( statement, index, lower, includeLower, upper,
                includeUpper );
    }

    @Override
    public PrimitiveLongIterator nodesGetFromIndexRangeSeekByPrefix( IndexDescriptor index, String prefix )
            throws IndexNotFoundKernelException
    {
        statement.assertOpen();
        return dataRead().nodesGetFromIndexRangeSeekByPrefix( statement, index, prefix );
    }

    @Override
    public PrimitiveLongIterator nodesGetFromIndexScan( IndexDescriptor index )
            throws IndexNotFoundKernelException
    {
        statement.assertOpen();
        return dataRead().nodesGetFromIndexScan( statement, index );
    }

    @Override
    public long nodeGetFromUniqueIndexSeek( IndexDescriptor index, Object value )
            throws IndexNotFoundKernelException, IndexBrokenKernelException
    {
        statement.assertOpen();
        return dataRead().nodeGetFromUniqueIndexSeek( statement, index, value );
    }

    @Override
    public boolean nodeExists( long nodeId )
    {
        statement.assertOpen();
        try ( Cursor<NodeItem> cursor = nodeCursor( nodeId ) )
        {
            return cursor.next();
        }
    }

    @Override
    public boolean relationshipExists( long relId )
    {
        statement.assertOpen();
        try ( Cursor<RelationshipItem> cursor = relationshipCursor( relId ) )
        {
            return cursor.next();
        }
    }

    @Override
    public boolean nodeHasLabel( long nodeId, int labelId ) throws EntityNotFoundException
    {
        statement.assertOpen();

        if ( labelId == StatementConstants.NO_SUCH_LABEL )
        {
            return false;
        }

        try ( Cursor<NodeItem> node = getNodeCursor( nodeId ) )
        {
            return node.get().hasLabel( labelId );
        }
    }

    @Override
    public PrimitiveIntIterator nodeGetLabels( long nodeId ) throws EntityNotFoundException
    {
        statement.assertOpen();
        try ( Cursor<NodeItem> node = getNodeCursor( nodeId ) )
        {
            return node.get().getLabels();
        }
    }

    @Override
    public boolean nodeHasProperty( long nodeId, int propertyKeyId ) throws EntityNotFoundException
    {
        statement.assertOpen();
        if ( propertyKeyId == StatementConstants.NO_SUCH_PROPERTY_KEY )
        {
            return false;
        }
        try ( Cursor<NodeItem> node = getNodeCursor( nodeId ) )
        {
            return node.get().hasProperty( propertyKeyId );
        }
    }

    @Override
    public Object nodeGetProperty( long nodeId, int propertyKeyId ) throws EntityNotFoundException
    {
        statement.assertOpen();
        if ( propertyKeyId == StatementConstants.NO_SUCH_PROPERTY_KEY )
        {
            return null;
        }
        try ( Cursor<NodeItem> node = getNodeCursor( nodeId ) )
        {
            return node.get().getProperty( propertyKeyId );
        }
    }

    @Override
    public RelationshipIterator nodeGetRelationships( long nodeId, Direction direction, int[] relTypes )
            throws EntityNotFoundException
    {
        statement.assertOpen();
        try ( Cursor<NodeItem> node = getNodeCursor( nodeId ) )
        {
            return node.get().getRelationships( direction, relTypes );
        }
    }

    @Override
    public RelationshipIterator nodeGetRelationships( long nodeId, Direction direction )
            throws EntityNotFoundException
    {
        statement.assertOpen();
        try ( Cursor<NodeItem> node = getNodeCursor( nodeId ) )
        {
            return node.get().getRelationships( direction );
        }
    }

    @Override
    public int nodeGetDegree( long nodeId, Direction direction, int relType ) throws EntityNotFoundException
    {
        statement.assertOpen();
        try ( Cursor<NodeItem> node = getNodeCursor( nodeId ) )
        {
            return node.get().degree( direction, relType );
        }
    }

    @Override
    public int nodeGetDegree( long nodeId, Direction direction ) throws EntityNotFoundException
    {
        statement.assertOpen();
        try ( Cursor<NodeItem> node = getNodeCursor( nodeId ) )
        {
            return node.get().degree( direction );
        }
    }

    @Override
    public PrimitiveIntIterator nodeGetRelationshipTypes( long nodeId ) throws EntityNotFoundException
    {
        statement.assertOpen();
        try ( Cursor<NodeItem> node = getNodeCursor( nodeId ) )
        {
            return node.get().getRelationshipTypes();
        }
    }

    @Override
    public boolean relationshipHasProperty( long relationshipId, int propertyKeyId ) throws EntityNotFoundException
    {
        statement.assertOpen();
        if ( propertyKeyId == StatementConstants.NO_SUCH_PROPERTY_KEY )
        {
            return false;
        }
        try ( Cursor<RelationshipItem> relationship = getRelationshipCursor( relationshipId ) )
        {
            return relationship.get().hasProperty( propertyKeyId );
        }
    }

    @Override
    public Object relationshipGetProperty( long relationshipId, int propertyKeyId ) throws EntityNotFoundException
    {
        statement.assertOpen();
        if ( propertyKeyId == StatementConstants.NO_SUCH_PROPERTY_KEY )
        {
            return null;
        }
        try ( Cursor<RelationshipItem> relationship = getRelationshipCursor( relationshipId ) )
        {
            return relationship.get().getProperty( propertyKeyId );
        }
    }

    @Override
    public boolean graphHasProperty( int propertyKeyId )
    {
        statement.assertOpen();
        if ( propertyKeyId == StatementConstants.NO_SUCH_PROPERTY_KEY )
        {
            return false;
        }
        return dataRead().graphHasProperty( statement, propertyKeyId );
    }

    @Override
    public Object graphGetProperty( int propertyKeyId )
    {
        statement.assertOpen();
        if ( propertyKeyId == StatementConstants.NO_SUCH_PROPERTY_KEY )
        {
            return null;
        }
        return dataRead().graphGetProperty( statement, propertyKeyId );
    }

    @Override
    public PrimitiveIntIterator nodeGetPropertyKeys( long nodeId ) throws EntityNotFoundException
    {
        statement.assertOpen();
        try ( Cursor<NodeItem> node = getNodeCursor( nodeId ) )
        {
            return node.get().getPropertyKeys();
        }
    }

    @Override
    public PrimitiveIntIterator relationshipGetPropertyKeys( long relationshipId ) throws EntityNotFoundException
    {
        statement.assertOpen();
        try ( Cursor<RelationshipItem> relationship = getRelationshipCursor( relationshipId ) )
        {
            return relationship.get().getPropertyKeys();
        }
    }

    @Override
    public PrimitiveIntIterator graphGetPropertyKeys()
    {
        statement.assertOpen();
        return dataRead().graphGetPropertyKeys( statement );
    }

    @Override
    public <EXCEPTION extends Exception> void relationshipVisit( long relId,
            RelationshipVisitor<EXCEPTION> visitor ) throws EntityNotFoundException, EXCEPTION
    {
        statement.assertOpen();
        dataRead().relationshipVisit( statement, relId, visitor );
    }

    // </DataRead>

    // <DataReadCursors>
    @Override
    public Cursor<NodeItem> nodeCursor( long nodeId )
    {
        statement.assertOpen();
        return dataRead().nodeCursor( statement, nodeId );
    }

    @Override
    public Cursor<RelationshipItem> relationshipCursor( long relId )
    {
        statement.assertOpen();
        return dataRead().relationshipCursor( statement, relId );
    }

    @Override
    public Cursor<NodeItem> nodeCursorGetAll()
    {
        statement.assertOpen();
        return dataRead().nodeCursorGetAll( statement );
    }

    @Override
    public Cursor<RelationshipItem> relationshipCursorGetAll()
    {
        statement.assertOpen();
        return dataRead().relationshipCursorGetAll( statement );
    }

    @Override
    public Cursor<NodeItem> nodeCursorGetForLabel( int labelId )
    {
        statement.assertOpen();
        return dataRead().nodeCursorGetForLabel( statement, labelId );
    }

    @Override
    public Cursor<NodeItem> nodeCursorGetFromIndexSeek( IndexDescriptor index,
            Object value ) throws IndexNotFoundKernelException
    {
        statement.assertOpen();
        return dataRead().nodeCursorGetFromIndexSeek( statement, index, value );
    }

    @Override
    public Cursor<NodeItem> nodeCursorGetFromIndexScan( IndexDescriptor index ) throws IndexNotFoundKernelException
    {
        statement.assertOpen();
        return dataRead().nodeCursorGetFromIndexScan( statement, index );
    }

    @Override
    public Cursor<NodeItem> nodeCursorGetFromIndexRangeSeekByNumber( IndexDescriptor index,
            Number lower, boolean includeLower,
            Number upper, boolean includeUpper )
            throws IndexNotFoundKernelException
    {
        statement.assertOpen();
        return dataRead().nodeCursorGetFromIndexRangeSeekByNumber( statement, index, lower, includeLower, upper,
                includeUpper );
    }

    @Override
    public Cursor<NodeItem> nodeCursorGetFromIndexRangeSeekByString( IndexDescriptor index,
            String lower, boolean includeLower,
            String upper, boolean includeUpper )
            throws IndexNotFoundKernelException
    {
        statement.assertOpen();
        return dataRead().nodeCursorGetFromIndexRangeSeekByString( statement, index, lower, includeLower, upper,
                includeUpper );
    }

    @Override
    public Cursor<NodeItem> nodeCursorGetFromIndexRangeSeekByPrefix( IndexDescriptor index, String prefix )
            throws IndexNotFoundKernelException
    {
        statement.assertOpen();
        return dataRead().nodeCursorGetFromIndexRangeSeekByPrefix( statement, index, prefix );
    }

    @Override
    public Cursor<NodeItem> nodeCursorGetFromUniqueIndexSeek( IndexDescriptor index, Object value )
            throws IndexNotFoundKernelException, IndexBrokenKernelException
    {
        statement.assertOpen();
        return dataRead().nodeCursorGetFromUniqueIndexSeek( statement, index, value );
    }

    // </DataReadCursors>

    // <SchemaRead>
    @Override
    public IndexDescriptor indexesGetForLabelAndPropertyKey( int labelId, int propertyKeyId )
            throws SchemaRuleNotFoundException
    {
        statement.assertOpen();
        IndexDescriptor descriptor = schemaRead().indexesGetForLabelAndPropertyKey( statement, labelId, propertyKeyId );
        if ( descriptor == null )
        {
            throw SchemaRuleNotFoundException.forNode( labelId, propertyKeyId, "not found" );
        }
        return descriptor;
    }

    @Override
    public Iterator<IndexDescriptor> indexesGetForLabel( int labelId )
    {
        statement.assertOpen();
        return schemaRead().indexesGetForLabel( statement, labelId );
    }

    @Override
    public Iterator<IndexDescriptor> indexesGetAll()
    {
        statement.assertOpen();
        return schemaRead().indexesGetAll( statement );
    }

    @Override
    public IndexDescriptor uniqueIndexGetForLabelAndPropertyKey( int labelId, int propertyKeyId )
            throws SchemaRuleNotFoundException

    {
        IndexDescriptor result = null;
        Iterator<IndexDescriptor> indexes = uniqueIndexesGetForLabel( labelId );
        while ( indexes.hasNext() )
        {
            IndexDescriptor index = indexes.next();
            if ( index.getPropertyKeyId() == propertyKeyId )
            {
                if ( null == result )
                {
                    result = index;
                }
                else
                {
                    throw SchemaRuleNotFoundException.forNode( labelId, propertyKeyId, "duplicate uniqueness index" );
                }
            }
        }

        if ( null == result )
        {
            throw SchemaRuleNotFoundException.forNode( labelId, propertyKeyId, "uniqueness index not found" );
        }

        return result;
    }

    @Override
    public Iterator<IndexDescriptor> uniqueIndexesGetForLabel( int labelId )
    {
        statement.assertOpen();
        return schemaRead().uniqueIndexesGetForLabel( statement, labelId );
    }

    @Override
    public Long indexGetOwningUniquenessConstraintId( IndexDescriptor index ) throws SchemaRuleNotFoundException
    {
        statement.assertOpen();
        return schemaRead().indexGetOwningUniquenessConstraintId( statement, index );
    }

    @Override
    public Iterator<IndexDescriptor> uniqueIndexesGetAll()
    {
        statement.assertOpen();
        return schemaRead().uniqueIndexesGetAll( statement );
    }

    @Override
    public InternalIndexState indexGetState( IndexDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        statement.assertOpen();
        return schemaRead().indexGetState( statement, descriptor );
    }

    @Override
    public long indexSize( IndexDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        statement.assertOpen();
        return schemaRead().indexSize( statement, descriptor );
    }

    @Override
    public double indexUniqueValuesSelectivity( IndexDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        statement.assertOpen();
        return schemaRead().indexUniqueValuesPercentage( statement, descriptor );
    }

    @Override
    public String indexGetFailure( IndexDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        statement.assertOpen();
        return schemaRead().indexGetFailure( statement, descriptor );
    }

    @Override
    public Iterator<NodePropertyConstraint> constraintsGetForLabelAndPropertyKey( int labelId, int propertyKeyId )
    {
        statement.assertOpen();
        return schemaRead().constraintsGetForLabelAndPropertyKey( statement, labelId, propertyKeyId );
    }

    @Override
    public Iterator<NodePropertyConstraint> constraintsGetForLabel( int labelId )
    {
        statement.assertOpen();
        return schemaRead().constraintsGetForLabel( statement, labelId );
    }

    @Override
    public Iterator<RelationshipPropertyConstraint> constraintsGetForRelationshipType( int typeId )
    {
        statement.assertOpen();
        return schemaRead().constraintsGetForRelationshipType( statement, typeId );
    }

    @Override
    public Iterator<RelationshipPropertyConstraint> constraintsGetForRelationshipTypeAndPropertyKey( int typeId,
            int propertyKeyId )
    {
        statement.assertOpen();
        return schemaRead().constraintsGetForRelationshipTypeAndPropertyKey( statement, typeId, propertyKeyId );
    }

    @Override
    public Iterator<PropertyConstraint> constraintsGetAll()
    {
        statement.assertOpen();
        return schemaRead().constraintsGetAll( statement );
    }
    // </SchemaRead>

    // <TokenRead>
    @Override
    public int labelGetForName( String labelName )
    {
        statement.assertOpen();
        return tokenRead().labelGetForName( statement, labelName );
    }

    @Override
    public String labelGetName( int labelId ) throws LabelNotFoundKernelException
    {
        statement.assertOpen();
        return tokenRead().labelGetName( statement, labelId );
    }

    @Override
    public int propertyKeyGetForName( String propertyKeyName )
    {
        statement.assertOpen();
        return tokenRead().propertyKeyGetForName( statement, propertyKeyName );
    }

    @Override
    public String propertyKeyGetName( int propertyKeyId ) throws PropertyKeyIdNotFoundKernelException
    {
        statement.assertOpen();
        return tokenRead().propertyKeyGetName( statement, propertyKeyId );
    }

    @Override
    public Iterator<Token> propertyKeyGetAllTokens()
    {
        statement.assertOpen();
        return tokenRead().propertyKeyGetAllTokens( statement );
    }

    @Override
    public Iterator<Token> labelsGetAllTokens()
    {
        statement.assertOpen();
        return tokenRead().labelsGetAllTokens( statement );
    }

    @Override
    public int relationshipTypeGetForName( String relationshipTypeName )
    {
        statement.assertOpen();
        return tokenRead().relationshipTypeGetForName( statement, relationshipTypeName );
    }

    @Override
    public String relationshipTypeGetName( int relationshipTypeId ) throws RelationshipTypeIdNotFoundKernelException
    {
        statement.assertOpen();
        return tokenRead().relationshipTypeGetName( statement, relationshipTypeId );
    }
    // </TokenRead>

    // <TokenWrite>
    @Override
    public int labelGetOrCreateForName( String labelName ) throws IllegalTokenNameException, TooManyLabelsException
    {
        statement.assertOpen();
        return tokenWrite().labelGetOrCreateForName( statement, labelName );
    }

    @Override
    public int propertyKeyGetOrCreateForName( String propertyKeyName ) throws IllegalTokenNameException
    {
        statement.assertOpen();
        return tokenWrite().propertyKeyGetOrCreateForName( statement,
                propertyKeyName );
    }

    @Override
    public int relationshipTypeGetOrCreateForName( String relationshipTypeName ) throws IllegalTokenNameException
    {
        statement.assertOpen();
        return tokenWrite().relationshipTypeGetOrCreateForName( statement, relationshipTypeName );
    }

    @Override
    public void labelCreateForName( String labelName, int id ) throws
            IllegalTokenNameException, TooManyLabelsException
    {
        statement.assertOpen();
        tokenWrite().labelCreateForName( statement, labelName, id );
    }

    @Override
    public void propertyKeyCreateForName( String propertyKeyName,
            int id ) throws
            IllegalTokenNameException
    {
        statement.assertOpen();
        tokenWrite().propertyKeyCreateForName( statement, propertyKeyName, id );
    }

    @Override
    public void relationshipTypeCreateForName( String relationshipTypeName,
            int id ) throws
            IllegalTokenNameException
    {
        statement.assertOpen();
        tokenWrite().relationshipTypeCreateForName( statement,
                relationshipTypeName, id );
    }


    // </TokenWrite>

    // <SchemaState>
    @Override
    public <K, V> V schemaStateGetOrCreate( K key, Function<K, V> creator )
    {
        return schemaState().schemaStateGetOrCreate( statement, key, creator );
    }


    @Override
    public void schemaStateFlush()
    {
        schemaState().schemaStateFlush( statement );
    }
    // </SchemaState>

    // <DataWrite>
    @Override
    public long nodeCreate()
    {
        statement.assertOpen();
        return dataWrite().nodeCreate( statement );
    }

    @Override
    public void nodeDelete( long nodeId ) throws EntityNotFoundException
    {
        statement.assertOpen();

        try ( Cursor<NodeItem> node = getNodeCursor( nodeId ) )
        {
            dataWrite().nodeDelete( statement, node.get() );
        }
    }

    @Override
    public long relationshipCreate( int relationshipTypeId, long startNodeId, long endNodeId )
            throws RelationshipTypeIdNotFoundKernelException, EntityNotFoundException
    {
        statement.assertOpen();

        try ( Cursor<NodeItem> startNode = getNodeCursor( startNodeId ) )
        {
            try ( Cursor<NodeItem> endNode = getNodeCursor( endNodeId ) )
            {
                return dataWrite().relationshipCreate( statement, relationshipTypeId, startNode.get(), endNode.get() );
            }
        }

    }

    @Override
    public void relationshipDelete( long relationshipId ) throws EntityNotFoundException
    {
        statement.assertOpen();

        try ( Cursor<RelationshipItem> relationship = getRelationshipCursor( relationshipId ) )
        {
            dataWrite().relationshipDelete( statement, relationship.get() );
        }
    }

    @Override
    public boolean nodeAddLabel( long nodeId, int labelId )
            throws EntityNotFoundException, ConstraintValidationKernelException
    {
        statement.assertOpen();

        try ( Cursor<NodeItem> node = getNodeCursor( nodeId ) )
        {
            return dataWrite().nodeAddLabel( statement, node.get(), labelId );
        }
    }

    @Override
    public boolean nodeRemoveLabel( long nodeId, int labelId ) throws EntityNotFoundException
    {
        statement.assertOpen();

        try ( Cursor<NodeItem> node = getNodeCursor( nodeId ) )
        {
            return dataWrite().nodeRemoveLabel( statement, node.get(), labelId );
        }
    }

    @Override
    public Property nodeSetProperty( long nodeId, DefinedProperty property )
            throws EntityNotFoundException, ConstraintValidationKernelException
    {
        statement.assertOpen();

        try ( Cursor<NodeItem> node = getNodeCursor( nodeId ) )
        {
            return dataWrite().nodeSetProperty( statement, node.get(), property );
        }
    }

    @Override
    public Property relationshipSetProperty( long relationshipId, DefinedProperty property )
            throws EntityNotFoundException
    {
        statement.assertOpen();

        try ( Cursor<RelationshipItem> relationship = getRelationshipCursor( relationshipId ) )
        {
            return dataWrite().relationshipSetProperty( statement, relationship.get(), property );
        }
    }

    @Override
    public Property graphSetProperty( DefinedProperty property )
    {
        statement.assertOpen();
        return dataWrite().graphSetProperty( statement, property );
    }

    @Override
    public Property nodeRemoveProperty( long nodeId, int propertyKeyId ) throws EntityNotFoundException
    {
        statement.assertOpen();

        try ( Cursor<NodeItem> node = getNodeCursor( nodeId ) )
        {
            return dataWrite().nodeRemoveProperty( statement, node.get(), propertyKeyId );
        }
    }

    @Override
    public Property relationshipRemoveProperty( long relationshipId, int propertyKeyId ) throws EntityNotFoundException
    {
        statement.assertOpen();

        try ( Cursor<RelationshipItem> relationship = getRelationshipCursor( relationshipId ) )
        {
            return dataWrite().relationshipRemoveProperty( statement, relationship.get(), propertyKeyId );
        }
    }

    @Override
    public Property graphRemoveProperty( int propertyKeyId )
    {
        statement.assertOpen();
        return dataWrite().graphRemoveProperty( statement, propertyKeyId );
    }
    // </DataWrite>

    // <SchemaWrite>
    @Override
    public IndexDescriptor indexCreate( int labelId, int propertyKeyId )
            throws AlreadyIndexedException, AlreadyConstrainedException
    {
        statement.assertOpen();
        return schemaWrite().indexCreate( statement, labelId, propertyKeyId );
    }

    @Override
    public void indexDrop( IndexDescriptor descriptor ) throws DropIndexFailureException
    {
        statement.assertOpen();
        schemaWrite().indexDrop( statement, descriptor );
    }

    @Override
    public UniquenessConstraint uniquePropertyConstraintCreate( int labelId, int propertyKeyId )
            throws CreateConstraintFailureException, AlreadyConstrainedException, AlreadyIndexedException
    {
        statement.assertOpen();
        return schemaWrite().uniquePropertyConstraintCreate( statement, labelId, propertyKeyId );
    }

    @Override
    public MandatoryNodePropertyConstraint mandatoryNodePropertyConstraintCreate( int labelId, int propertyKeyId )
            throws CreateConstraintFailureException, AlreadyConstrainedException
    {
        statement.assertOpen();
        return schemaWrite().mandatoryNodePropertyConstraintCreate( statement, labelId, propertyKeyId );
    }

    @Override
    public MandatoryRelationshipPropertyConstraint mandatoryRelationshipPropertyConstraintCreate(
            int relTypeId, int propertyKeyId )
            throws CreateConstraintFailureException, AlreadyConstrainedException
    {
        statement.assertOpen();
        return schemaWrite().mandatoryRelationshipPropertyConstraintCreate( statement, relTypeId, propertyKeyId );
    }

    @Override
    public void constraintDrop( NodePropertyConstraint constraint ) throws DropConstraintFailureException
    {
        statement.assertOpen();
        schemaWrite().constraintDrop( statement, constraint );
    }

    @Override
    public void constraintDrop( RelationshipPropertyConstraint constraint ) throws DropConstraintFailureException
    {
        statement.assertOpen();
        schemaWrite().constraintDrop( statement, constraint );
    }

    @Override
    public void uniqueIndexDrop( IndexDescriptor descriptor ) throws DropIndexFailureException
    {
        statement.assertOpen();
        schemaWrite().uniqueIndexDrop( statement, descriptor );
    }
    // </SchemaWrite>


    // <Locking>
    @Override
    public void acquireExclusive( Locks.ResourceType type, long id )
    {
        statement.assertOpen();
        locking().acquireExclusive( statement, type, id );
    }

    @Override
    public void acquireShared( Locks.ResourceType type, long id )
    {
        statement.assertOpen();
        locking().acquireShared( statement, type, id );
    }

    @Override
    public void releaseExclusive( Locks.ResourceType type, long id )
    {
        statement.assertOpen();
        locking().releaseExclusive( statement, type, id );
    }

    @Override
    public void releaseShared( Locks.ResourceType type, long id )
    {
        statement.assertOpen();
        locking().releaseShared( statement, type, id );
    }
    // </Locking>

    // <Legacy index>
    @Override
    public LegacyIndexHits nodeLegacyIndexGet( String indexName, String key, Object value )
            throws LegacyIndexNotFoundKernelException
    {
        statement.assertOpen();
        return legacyIndexRead().nodeLegacyIndexGet( statement, indexName, key, value );
    }

    @Override
    public LegacyIndexHits nodeLegacyIndexQuery( String indexName, String key, Object queryOrQueryObject )
            throws LegacyIndexNotFoundKernelException
    {
        statement.assertOpen();
        return legacyIndexRead().nodeLegacyIndexQuery( statement, indexName, key, queryOrQueryObject );
    }

    @Override
    public LegacyIndexHits nodeLegacyIndexQuery( String indexName, Object queryOrQueryObject )
            throws LegacyIndexNotFoundKernelException
    {
        statement.assertOpen();
        return legacyIndexRead().nodeLegacyIndexQuery( statement, indexName, queryOrQueryObject );
    }

    @Override
    public LegacyIndexHits relationshipLegacyIndexGet( String indexName, String key, Object value,
            long startNode, long endNode ) throws LegacyIndexNotFoundKernelException
    {
        statement.assertOpen();
        return legacyIndexRead().relationshipLegacyIndexGet( statement, indexName, key, value, startNode, endNode );
    }

    @Override
    public LegacyIndexHits relationshipLegacyIndexQuery( String indexName, String key, Object queryOrQueryObject,
            long startNode, long endNode ) throws LegacyIndexNotFoundKernelException
    {
        statement.assertOpen();
        return legacyIndexRead().relationshipLegacyIndexQuery( statement, indexName, key, queryOrQueryObject,
                startNode, endNode );
    }

    @Override
    public LegacyIndexHits relationshipLegacyIndexQuery( String indexName, Object queryOrQueryObject,
            long startNode, long endNode ) throws LegacyIndexNotFoundKernelException
    {
        statement.assertOpen();
        return legacyIndexRead().relationshipLegacyIndexQuery( statement, indexName, queryOrQueryObject,
                startNode, endNode );
    }

    @Override
    public void nodeLegacyIndexCreateLazily( String indexName, Map<String, String> customConfig )
    {
        statement.assertOpen();
        legacyIndexWrite().nodeLegacyIndexCreateLazily( statement, indexName, customConfig );
    }

    @Override
    public void nodeLegacyIndexCreate( String indexName, Map<String, String> customConfig )
    {
        statement.assertOpen();

        legacyIndexWrite().nodeLegacyIndexCreate( statement, indexName, customConfig );
    }

    @Override
    public void relationshipLegacyIndexCreateLazily( String indexName, Map<String, String> customConfig )
    {
        statement.assertOpen();
        legacyIndexWrite().relationshipLegacyIndexCreateLazily( statement, indexName, customConfig );
    }

    @Override
    public void relationshipLegacyIndexCreate( String indexName, Map<String, String> customConfig )
    {
        statement.assertOpen();

        legacyIndexWrite().relationshipLegacyIndexCreate( statement, indexName, customConfig );
    }

    @Override
    public void nodeAddToLegacyIndex( String indexName, long node, String key, Object value )
            throws EntityNotFoundException, LegacyIndexNotFoundKernelException
    {
        statement.assertOpen();
        legacyIndexWrite().nodeAddToLegacyIndex( statement, indexName, node, key, value );
    }

    @Override
    public void nodeRemoveFromLegacyIndex( String indexName, long node, String key, Object value )
            throws LegacyIndexNotFoundKernelException
    {
        statement.assertOpen();
        legacyIndexWrite().nodeRemoveFromLegacyIndex( statement, indexName, node, key, value );
    }

    @Override
    public void nodeRemoveFromLegacyIndex( String indexName, long node, String key )
            throws LegacyIndexNotFoundKernelException
    {
        statement.assertOpen();
        legacyIndexWrite().nodeRemoveFromLegacyIndex( statement, indexName, node, key );
    }

    @Override
    public void nodeRemoveFromLegacyIndex( String indexName, long node ) throws LegacyIndexNotFoundKernelException
    {
        statement.assertOpen();
        legacyIndexWrite().nodeRemoveFromLegacyIndex( statement, indexName, node );
    }

    @Override
    public void relationshipAddToLegacyIndex( String indexName, long relationship, String key, Object value )
            throws EntityNotFoundException, LegacyIndexNotFoundKernelException
    {
        statement.assertOpen();
        legacyIndexWrite().relationshipAddToLegacyIndex( statement, indexName, relationship, key, value );
    }

    @Override
    public void relationshipRemoveFromLegacyIndex( String indexName, long relationship, String key, Object value )
            throws EntityNotFoundException, LegacyIndexNotFoundKernelException
    {
        statement.assertOpen();
        legacyIndexWrite().relationshipRemoveFromLegacyIndex( statement, indexName, relationship, key, value );
    }

    @Override
    public void relationshipRemoveFromLegacyIndex( String indexName, long relationship, String key )
            throws LegacyIndexNotFoundKernelException, EntityNotFoundException
    {
        statement.assertOpen();
        legacyIndexWrite().relationshipRemoveFromLegacyIndex( statement, indexName, relationship, key );
    }

    @Override
    public void relationshipRemoveFromLegacyIndex( String indexName, long relationship )
            throws LegacyIndexNotFoundKernelException, EntityNotFoundException
    {
        statement.assertOpen();
        legacyIndexWrite().relationshipRemoveFromLegacyIndex( statement, indexName, relationship );
    }

    @Override
    public void nodeLegacyIndexDrop( String indexName ) throws LegacyIndexNotFoundKernelException
    {
        statement.assertOpen();
        legacyIndexWrite().nodeLegacyIndexDrop( statement, indexName );
    }

    @Override
    public void relationshipLegacyIndexDrop( String indexName ) throws LegacyIndexNotFoundKernelException
    {
        statement.assertOpen();
        legacyIndexWrite().relationshipLegacyIndexDrop( statement, indexName );
    }

    @Override
    public Map<String, String> nodeLegacyIndexGetConfiguration( String indexName )
            throws LegacyIndexNotFoundKernelException
    {
        statement.assertOpen();
        return legacyIndexRead().nodeLegacyIndexGetConfiguration( statement, indexName );
    }

    @Override
    public Map<String, String> relationshipLegacyIndexGetConfiguration( String indexName )
            throws LegacyIndexNotFoundKernelException
    {
        statement.assertOpen();
        return legacyIndexRead().relationshipLegacyIndexGetConfiguration( statement, indexName );
    }

    @Override
    public String nodeLegacyIndexSetConfiguration( String indexName, String key, String value )
            throws LegacyIndexNotFoundKernelException
    {
        statement.assertOpen();
        return legacyIndexWrite().nodeLegacyIndexSetConfiguration( statement, indexName, key, value );
    }

    @Override
    public String relationshipLegacyIndexSetConfiguration( String indexName, String key, String value )
            throws LegacyIndexNotFoundKernelException
    {
        statement.assertOpen();
        return legacyIndexWrite().relationshipLegacyIndexSetConfiguration( statement, indexName, key, value );
    }

    @Override
    public String nodeLegacyIndexRemoveConfiguration( String indexName, String key )
            throws LegacyIndexNotFoundKernelException
    {
        statement.assertOpen();
        return legacyIndexWrite().nodeLegacyIndexRemoveConfiguration( statement, indexName, key );
    }

    @Override
    public String relationshipLegacyIndexRemoveConfiguration( String indexName, String key )
            throws LegacyIndexNotFoundKernelException
    {
        statement.assertOpen();
        return legacyIndexWrite().relationshipLegacyIndexRemoveConfiguration( statement, indexName, key );
    }

    @Override
    public String[] nodeLegacyIndexesGetAll()
    {
        statement.assertOpen();
        return legacyIndexRead().nodeLegacyIndexesGetAll( statement );
    }

    @Override
    public String[] relationshipLegacyIndexesGetAll()
    {
        statement.assertOpen();
        return legacyIndexRead().relationshipLegacyIndexesGetAll( statement );
    }
    // </Legacy index>

    // <Counts>

    @Override
    public long countsForNode( int labelId )
    {
        statement.assertOpen();
        return counting().countsForNode( statement, labelId );
    }

    @Override
    public long countsForRelationship( int startLabelId, int typeId, int endLabelId )
    {
        statement.assertOpen();
        return counting().countsForRelationship( statement, startLabelId, typeId, endLabelId );
    }

    // </Counts>

    private Cursor<NodeItem> getNodeCursor( long nodeId ) throws EntityNotFoundException
    {
        Cursor<NodeItem> node = dataRead().nodeCursor( statement, nodeId );

        if ( !node.next() )
        {
            node.close();
            throw new EntityNotFoundException( EntityType.NODE, nodeId );
        }
        else
        {
            return node;
        }
    }

    private Cursor<RelationshipItem> getRelationshipCursor( long relationshipId ) throws EntityNotFoundException
    {
        Cursor<RelationshipItem> relationship = dataRead().relationshipCursor( statement, relationshipId );

        if ( !relationship.next() )
        {
            relationship.close();
            throw new EntityNotFoundException( EntityType.RELATIONSHIP, relationshipId );
        }
        else
        {
            return relationship;
        }
    }
}

/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

import org.neo4j.collection.RawIterator;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.cursor.Cursor;
import org.neo4j.graphdb.Direction;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.api.DataWriteOperations;
import org.neo4j.kernel.api.ExecutionStatisticsOperations;
import org.neo4j.kernel.api.ExplicitIndexHits;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.ProcedureCallOperations;
import org.neo4j.kernel.api.QueryRegistryOperations;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.SchemaWriteOperations;
import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.kernel.api.TokenWriteOperations;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.LabelNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.exceptions.PropertyKeyIdNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.RelationshipTypeIdNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.explicitindex.AutoIndexingKernelException;
import org.neo4j.kernel.api.exceptions.explicitindex.ExplicitIndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexNotApplicableKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.schema.AlreadyConstrainedException;
import org.neo4j.kernel.api.exceptions.schema.AlreadyIndexedException;
import org.neo4j.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.kernel.api.exceptions.schema.DropConstraintFailureException;
import org.neo4j.kernel.api.exceptions.schema.DropIndexFailureException;
import org.neo4j.kernel.api.exceptions.schema.IllegalTokenNameException;
import org.neo4j.kernel.api.exceptions.schema.IndexBrokenKernelException;
import org.neo4j.kernel.api.exceptions.schema.RepeatedPropertyInCompositeSchemaException;
import org.neo4j.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.exceptions.schema.TooManyLabelsException;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.proc.BasicContext;
import org.neo4j.kernel.api.proc.CallableUserAggregationFunction;
import org.neo4j.kernel.api.proc.Context;
import org.neo4j.kernel.api.proc.ProcedureSignature;
import org.neo4j.kernel.api.proc.QualifiedName;
import org.neo4j.kernel.api.proc.UserFunctionSignature;
import org.neo4j.kernel.api.query.ExecutingQuery;
import org.neo4j.kernel.api.schema.IndexQuery;
import org.neo4j.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.kernel.api.schema.RelationTypeSchemaDescriptor;
import org.neo4j.kernel.api.schema.SchemaDescriptor;
import org.neo4j.kernel.api.schema.constaints.ConstraintDescriptor;
import org.neo4j.kernel.api.schema.constaints.NodeExistenceConstraintDescriptor;
import org.neo4j.kernel.api.schema.constaints.NodeKeyConstraintDescriptor;
import org.neo4j.kernel.api.schema.constaints.RelExistenceConstraintDescriptor;
import org.neo4j.kernel.api.schema.constaints.UniquenessConstraintDescriptor;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.api.security.AccessMode;
import org.neo4j.kernel.api.security.SecurityContext;
import org.neo4j.kernel.impl.api.operations.CountsOperations;
import org.neo4j.kernel.impl.api.operations.EntityReadOperations;
import org.neo4j.kernel.impl.api.operations.EntityWriteOperations;
import org.neo4j.kernel.impl.api.operations.ExplicitIndexReadOperations;
import org.neo4j.kernel.impl.api.operations.ExplicitIndexWriteOperations;
import org.neo4j.kernel.impl.api.operations.KeyReadOperations;
import org.neo4j.kernel.impl.api.operations.KeyWriteOperations;
import org.neo4j.kernel.impl.api.operations.LockOperations;
import org.neo4j.kernel.impl.api.operations.QueryRegistrationOperations;
import org.neo4j.kernel.impl.api.operations.SchemaReadOperations;
import org.neo4j.kernel.impl.api.operations.SchemaStateOperations;
import org.neo4j.kernel.impl.api.security.OverriddenAccessMode;
import org.neo4j.kernel.impl.api.security.RestrictedAccessMode;
import org.neo4j.kernel.impl.api.store.CursorRelationshipIterator;
import org.neo4j.kernel.impl.api.store.RelationshipIterator;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.impl.query.clientconnection.ClientConnectionInfo;
import org.neo4j.register.Register.DoubleLongRegister;
import org.neo4j.storageengine.api.NodeItem;
import org.neo4j.storageengine.api.PropertyItem;
import org.neo4j.storageengine.api.RelationshipItem;
import org.neo4j.storageengine.api.Token;
import org.neo4j.storageengine.api.lock.ResourceType;
import org.neo4j.storageengine.api.schema.PopulationProgress;
import org.neo4j.storageengine.api.schema.SchemaRule;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.MapValue;

import static java.lang.String.format;
import static org.neo4j.collection.primitive.PrimitiveIntCollections.deduplicate;

public class OperationsFacade
        implements ReadOperations, DataWriteOperations, TokenWriteOperations, SchemaWriteOperations,
        QueryRegistryOperations, ProcedureCallOperations, ExecutionStatisticsOperations
{
    private final KernelTransaction tx;
    private final KernelStatement statement;
    private final Procedures procedures;
    private StatementOperationParts operations;

    OperationsFacade( KernelTransaction tx, KernelStatement statement,
                      Procedures procedures, StatementOperationParts operationParts )
    {
        this.tx = tx;
        this.statement = statement;
        this.procedures = procedures;
        this.operations = operationParts;
    }

    final KeyReadOperations tokenRead()
    {
        return operations.keyReadOperations();
    }

    final KeyWriteOperations tokenWrite()
    {
        statement.assertAllows( AccessMode::allowsTokenCreates, "Token create" );
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

    final ExplicitIndexWriteOperations explicitIndexWrite()
    {
        return operations.explicitIndexWriteOperations();
    }

    final ExplicitIndexReadOperations explicitIndexRead()
    {
        return operations.explicitIndexReadOperations();
    }

    final SchemaReadOperations schemaRead()
    {
        return operations.schemaReadOperations();
    }

    final org.neo4j.kernel.impl.api.operations.SchemaWriteOperations schemaWrite()
    {
        return operations.schemaWriteOperations();
    }

    final QueryRegistrationOperations queryRegistrationOperations()
    {
        return operations.queryRegistrationOperations();
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
    public PrimitiveLongIterator indexQuery( IndexDescriptor index, IndexQuery... predicates )
            throws IndexNotFoundKernelException, IndexNotApplicableKernelException
    {
        statement.assertOpen();
        return dataRead().indexQuery( statement, index, predicates );
    }

    @Override
    public long nodeGetFromUniqueIndexSeek( IndexDescriptor index, IndexQuery.ExactPredicate... predicates )
            throws IndexNotFoundKernelException, IndexBrokenKernelException, IndexNotApplicableKernelException
    {
        statement.assertOpen();
        return dataRead().nodeGetFromUniqueIndexSeek( statement, index, predicates );
    }

    @Override
    public boolean nodeExists( long nodeId )
    {
        statement.assertOpen();
        return dataRead().nodeExists( statement, nodeId );
    }

    @Override
    public boolean nodeHasLabel( long nodeId, int labelId ) throws EntityNotFoundException
    {
        statement.assertOpen();

        if ( labelId == StatementConstants.NO_SUCH_LABEL )
        {
            return false;
        }

        try ( Cursor<NodeItem> node = dataRead().nodeCursorById( statement, nodeId ) )
        {
            return node.get().hasLabel( labelId );
        }
    }

    @Override
    public PrimitiveIntIterator nodeGetLabels( long nodeId ) throws EntityNotFoundException
    {
        statement.assertOpen();
        try ( Cursor<NodeItem> node = dataRead().nodeCursorById( statement, nodeId ) )
        {
            return node.get().labels().iterator();
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
        try ( Cursor<NodeItem> node = dataRead().nodeCursorById( statement, nodeId ) )
        {
            return dataRead().nodeHasProperty( statement, node.get(), propertyKeyId );
        }
    }

    @Override
    public Value nodeGetProperty( long nodeId, int propertyKeyId ) throws EntityNotFoundException
    {
        statement.assertOpen();
        if ( propertyKeyId == StatementConstants.NO_SUCH_PROPERTY_KEY )
        {
            return Values.NO_VALUE;
        }
        try ( Cursor<NodeItem> node = dataRead().nodeCursorById( statement, nodeId ) )
        {
            return dataRead().nodeGetProperty( statement, node.get(), propertyKeyId );
        }
    }

    @Override
    public RelationshipIterator nodeGetRelationships( long nodeId, Direction direction, int[] relTypes )
            throws EntityNotFoundException
    {
        statement.assertOpen();
        try ( Cursor<NodeItem> node = dataRead().nodeCursorById( statement, nodeId ) )
        {
            return new CursorRelationshipIterator( dataRead()
                    .nodeGetRelationships( statement, node.get(), direction( direction ), deduplicate( relTypes ) ) );
        }
    }

    private org.neo4j.storageengine.api.Direction direction( Direction direction )
    {
        switch ( direction )
        {
        case OUTGOING: return org.neo4j.storageengine.api.Direction.OUTGOING;
        case INCOMING: return org.neo4j.storageengine.api.Direction.INCOMING;
        case BOTH: return org.neo4j.storageengine.api.Direction.BOTH;
        default: throw new IllegalArgumentException( direction.name() );
        }
    }

    @Override
    public RelationshipIterator nodeGetRelationships( long nodeId, Direction direction )
            throws EntityNotFoundException
    {
        statement.assertOpen();
        try ( Cursor<NodeItem> node = dataRead().nodeCursorById( statement, nodeId ) )
        {
            return new CursorRelationshipIterator(
                    dataRead().nodeGetRelationships( statement, node.get(), direction( direction ) ) );
        }
    }

    @Override
    public int nodeGetDegree( long nodeId, Direction direction, int relType ) throws EntityNotFoundException
    {
        statement.assertOpen();
        try ( Cursor<NodeItem> node = dataRead().nodeCursorById( statement, nodeId ) )
        {
            return dataRead().degree( statement, node.get(), direction( direction ), relType );
        }
    }

    @Override
    public int nodeGetDegree( long nodeId, Direction direction ) throws EntityNotFoundException
    {
        statement.assertOpen();
        try ( Cursor<NodeItem> node = dataRead().nodeCursorById( statement, nodeId ) )
        {
            return dataRead().degree( statement, node.get(), direction( direction ) );
        }
    }

    @Override
    public boolean nodeIsDense( long nodeId ) throws EntityNotFoundException
    {
        statement.assertOpen();
        try ( Cursor<NodeItem> node = dataRead().nodeCursorById( statement, nodeId ) )
        {
            return node.get().isDense();
        }
    }

    @Override
    public PrimitiveIntIterator nodeGetRelationshipTypes( long nodeId ) throws EntityNotFoundException
    {
        statement.assertOpen();
        try ( Cursor<NodeItem> node = dataRead().nodeCursorById( statement, nodeId ) )
        {
            return dataRead().relationshipTypes( statement,node.get() ).iterator();
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
        try ( Cursor<RelationshipItem> relationship = dataRead().relationshipCursorById( statement, relationshipId ) )
        {
            return dataRead().relationshipHasProperty( statement, relationship.get(), propertyKeyId );
        }
    }

    @Override
    public Value relationshipGetProperty( long relationshipId, int propertyKeyId ) throws EntityNotFoundException
    {
        statement.assertOpen();
        if ( propertyKeyId == StatementConstants.NO_SUCH_PROPERTY_KEY )
        {
            return Values.NO_VALUE;
        }
        try ( Cursor<RelationshipItem> relationship = dataRead().relationshipCursorById( statement, relationshipId ) )
        {
            return dataRead().relationshipGetProperty( statement, relationship.get(), propertyKeyId );
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
    public Value graphGetProperty( int propertyKeyId )
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
        try ( Cursor<NodeItem> node = dataRead().nodeCursorById( statement, nodeId ) )
        {
            return dataRead().nodeGetPropertyKeys( statement, node.get() ).iterator();
        }
    }

    @Override
    public PrimitiveIntIterator relationshipGetPropertyKeys( long relationshipId ) throws EntityNotFoundException
    {
        statement.assertOpen();
        try ( Cursor<RelationshipItem> relationship = dataRead().relationshipCursorById( statement, relationshipId ) )
        {
            return dataRead().relationshipGetPropertyKeys( statement, relationship.get() ).iterator();
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

    @Override
    public long nodesGetCount()
    {
        statement.assertOpen();
        return dataRead().nodesGetCount( statement );
    }

    @Override
    public long relationshipsGetCount()
    {
        statement.assertOpen();
        return dataRead().relationshipsGetCount( statement );
    }

    @Override
    public ProcedureSignature procedureGet( QualifiedName name ) throws ProcedureException
    {
        statement.assertOpen();
        return procedures.procedure( name );
    }

    @Override
    public Optional<UserFunctionSignature> functionGet( QualifiedName name )
    {
        statement.assertOpen();
        return procedures.function( name );
    }

    @Override
    public Optional<UserFunctionSignature> aggregationFunctionGet( QualifiedName name )
    {
        statement.assertOpen();
        return procedures.aggregationFunction( name );
    }

    @Override
    public Set<UserFunctionSignature> functionsGetAll()
    {
        statement.assertOpen();
        return procedures.getAllFunctions();
    }

    @Override
    public Set<ProcedureSignature> proceduresGetAll()
    {
        statement.assertOpen();
        return procedures.getAllProcedures();
    }

    @Override
    public long nodesCountIndexed( IndexDescriptor index, long nodeId, Value value )
            throws IndexNotFoundKernelException, IndexBrokenKernelException
    {
        statement.assertOpen();
        return dataRead().nodesCountIndexed( statement, index, nodeId, value );
    }
    // </DataRead>

    // <DataReadCursors>
    @Override
    public Cursor<NodeItem> nodeCursorById( long nodeId ) throws EntityNotFoundException
    {
        statement.assertOpen();
        return dataRead().nodeCursorById( statement, nodeId );
    }

    @Override
    public Cursor<RelationshipItem> relationshipCursorById( long relId ) throws EntityNotFoundException
    {
        statement.assertOpen();
        return dataRead().relationshipCursorById( statement, relId );
    }

    @Override
    public Cursor<PropertyItem> nodeGetProperties( NodeItem node )
    {
        statement.assertOpen();
        return dataRead().nodeGetProperties( statement, node );
    }

    @Override
    public Cursor<PropertyItem> relationshipGetProperties( RelationshipItem relationship )
    {
        statement.assertOpen();
        return dataRead().relationshipGetProperties( statement, relationship );
    }

    // </DataReadCursors>

    // <SchemaRead>
    @Override
    public IndexDescriptor indexGetForSchema( LabelSchemaDescriptor descriptor )
            throws SchemaRuleNotFoundException
    {
        statement.assertOpen();
        IndexDescriptor indexDescriptor = schemaRead().indexGetForSchema( statement, descriptor );
        if ( indexDescriptor == null )
        {
            throw new SchemaRuleNotFoundException( SchemaRule.Kind.INDEX_RULE, descriptor );
        }
        return indexDescriptor;
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
    public Long indexGetOwningUniquenessConstraintId( IndexDescriptor index )
    {
        statement.assertOpen();
        return schemaRead().indexGetOwningUniquenessConstraintId( statement, index );
    }

    @Override
    public InternalIndexState indexGetState( IndexDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        statement.assertOpen();
        return schemaRead().indexGetState( statement, descriptor );
    }

    @Override
    public PopulationProgress indexGetPopulationProgress( IndexDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        statement.assertOpen();
        return schemaRead().indexGetPopulationProgress( statement, descriptor );
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
    public Iterator<ConstraintDescriptor> constraintsGetForSchema( SchemaDescriptor descriptor )
    {
        statement.assertOpen();
        return schemaRead().constraintsGetForSchema( statement, descriptor );
    }

    @Override
    public Iterator<ConstraintDescriptor> constraintsGetForLabel( int labelId )
    {
        statement.assertOpen();
        return schemaRead().constraintsGetForLabel( statement, labelId );
    }

    @Override
    public Iterator<ConstraintDescriptor> constraintsGetForRelationshipType( int typeId )
    {
        statement.assertOpen();
        return schemaRead().constraintsGetForRelationshipType( statement, typeId );
    }

    @Override
    public Iterator<ConstraintDescriptor> constraintsGetAll()
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
    public Iterator<Token> relationshipTypesGetAllTokens()
    {
        statement.assertOpen();
        return tokenRead().relationshipTypesGetAllTokens( statement );
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

    @Override
    public int labelCount()
    {
        statement.assertOpen();
        return tokenRead().labelCount( statement );
    }

    @Override
    public int propertyKeyCount()
    {
        statement.assertOpen();
        return tokenRead().propertyKeyCount( statement );
    }

    @Override
    public int relationshipTypeCount()
    {
        statement.assertOpen();
        return tokenRead().relationshipTypeCount( statement );
    }

    // </TokenRead>

    // <TokenWrite>
    @Override
    public int labelGetOrCreateForName( String labelName ) throws IllegalTokenNameException, TooManyLabelsException
    {
        statement.assertOpen();
        int id = tokenRead().labelGetForName( statement, labelName );
        if ( id != KeyReadOperations.NO_SUCH_LABEL )
        {
            return id;
        }

        return tokenWrite().labelGetOrCreateForName( statement, labelName );
    }

    @Override
    public int propertyKeyGetOrCreateForName( String propertyKeyName ) throws IllegalTokenNameException
    {
        statement.assertOpen();
        int id = tokenRead().propertyKeyGetForName( statement, propertyKeyName );
        if ( id != KeyReadOperations.NO_SUCH_PROPERTY_KEY )
        {
            return id;
        }
        return tokenWrite().propertyKeyGetOrCreateForName( statement,
                propertyKeyName );
    }

    @Override
    public int relationshipTypeGetOrCreateForName( String relationshipTypeName ) throws IllegalTokenNameException
    {
        statement.assertOpen();
        int id = tokenRead().relationshipTypeGetForName( statement, relationshipTypeName );
        if ( id != KeyReadOperations.NO_SUCH_RELATIONSHIP_TYPE )
        {
            return id;
        }
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
    public <K, V> V schemaStateGetOrCreate( K key, Function<K,V> creator )
    {
        return schemaState().schemaStateGetOrCreate( statement, key, creator );
    }

    @Override
    public <K, V> V schemaStateGet( K key )
    {
        return schemaState().schemaStateGet( statement, key );
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
    public void nodeDelete( long nodeId )
            throws EntityNotFoundException, InvalidTransactionTypeKernelException, AutoIndexingKernelException
    {
        statement.assertOpen();
        dataWrite().nodeDelete( statement, nodeId );
    }

    @Override
    public int nodeDetachDelete( long nodeId ) throws KernelException
    {
        statement.assertOpen();
        return dataWrite().nodeDetachDelete( statement, nodeId );
    }

    @Override
    public long relationshipCreate( int relationshipTypeId, long startNodeId, long endNodeId )
            throws RelationshipTypeIdNotFoundKernelException, EntityNotFoundException
    {
        statement.assertOpen();
        return dataWrite().relationshipCreate( statement, relationshipTypeId, startNodeId, endNodeId );
    }

    @Override
    public void relationshipDelete( long relationshipId )
            throws EntityNotFoundException, InvalidTransactionTypeKernelException, AutoIndexingKernelException
    {
        statement.assertOpen();
        dataWrite().relationshipDelete( statement, relationshipId );
    }

    @Override
    public boolean nodeAddLabel( long nodeId, int labelId )
            throws EntityNotFoundException, ConstraintValidationException
    {
        statement.assertOpen();
        return dataWrite().nodeAddLabel( statement, nodeId, labelId );
    }

    @Override
    public boolean nodeRemoveLabel( long nodeId, int labelId ) throws EntityNotFoundException
    {
        statement.assertOpen();
        return dataWrite().nodeRemoveLabel( statement, nodeId, labelId );
    }

    @Override
    public Value nodeSetProperty( long nodeId, int propertyKeyId, Value value )
            throws EntityNotFoundException, AutoIndexingKernelException,
                   InvalidTransactionTypeKernelException, ConstraintValidationException
    {
        statement.assertOpen();
        return dataWrite().nodeSetProperty( statement, nodeId, propertyKeyId, value );
    }

    @Override
    public Value relationshipSetProperty( long relationshipId, int propertyKeyId, Value value )
            throws EntityNotFoundException, AutoIndexingKernelException, InvalidTransactionTypeKernelException
    {
        statement.assertOpen();
        return dataWrite().relationshipSetProperty( statement, relationshipId, propertyKeyId, value );
    }

    @Override
    public Value graphSetProperty( int propertyKeyId, Value value )
    {
        statement.assertOpen();
        return dataWrite().graphSetProperty( statement, propertyKeyId, value );
    }

    @Override
    public Value nodeRemoveProperty( long nodeId, int propertyKeyId )
            throws EntityNotFoundException, AutoIndexingKernelException, InvalidTransactionTypeKernelException
    {
        statement.assertOpen();
        return dataWrite().nodeRemoveProperty( statement, nodeId, propertyKeyId );
    }

    @Override
    public Value relationshipRemoveProperty( long relationshipId, int propertyKeyId )
            throws EntityNotFoundException, AutoIndexingKernelException, InvalidTransactionTypeKernelException
    {
        statement.assertOpen();
        return dataWrite().relationshipRemoveProperty( statement, relationshipId, propertyKeyId );
    }

    @Override
    public Value graphRemoveProperty( int propertyKeyId )
    {
        statement.assertOpen();
        return dataWrite().graphRemoveProperty( statement, propertyKeyId );
    }

    // </DataWrite>

    // <SchemaWrite>
    @Override
    public IndexDescriptor indexCreate( LabelSchemaDescriptor descriptor )
            throws AlreadyIndexedException, AlreadyConstrainedException, RepeatedPropertyInCompositeSchemaException
    {
        statement.assertOpen();
        return schemaWrite().indexCreate( statement, descriptor );
    }

    @Override
    public void indexDrop( IndexDescriptor descriptor ) throws DropIndexFailureException
    {
        statement.assertOpen();
        schemaWrite().indexDrop( statement, descriptor );
    }

    @Override
    public NodeKeyConstraintDescriptor nodeKeyConstraintCreate( LabelSchemaDescriptor descriptor )
            throws CreateConstraintFailureException, AlreadyConstrainedException, AlreadyIndexedException,
            RepeatedPropertyInCompositeSchemaException
    {
        statement.assertOpen();
        return schemaWrite().nodeKeyConstraintCreate( statement, descriptor );
    }

    @Override
    public UniquenessConstraintDescriptor uniquePropertyConstraintCreate( LabelSchemaDescriptor descriptor )
            throws CreateConstraintFailureException, AlreadyConstrainedException, AlreadyIndexedException,
            RepeatedPropertyInCompositeSchemaException
    {
        statement.assertOpen();
        return schemaWrite().uniquePropertyConstraintCreate( statement, descriptor );
    }

    @Override
    public NodeExistenceConstraintDescriptor nodePropertyExistenceConstraintCreate( LabelSchemaDescriptor descriptor )
            throws CreateConstraintFailureException, AlreadyConstrainedException,
            RepeatedPropertyInCompositeSchemaException
    {
        statement.assertOpen();
        return schemaWrite().nodePropertyExistenceConstraintCreate( statement, descriptor );
    }

    @Override
    public RelExistenceConstraintDescriptor relationshipPropertyExistenceConstraintCreate(
            RelationTypeSchemaDescriptor descriptor )
            throws CreateConstraintFailureException, AlreadyConstrainedException,
            RepeatedPropertyInCompositeSchemaException
    {
        statement.assertOpen();
        return schemaWrite().relationshipPropertyExistenceConstraintCreate( statement, descriptor );
    }

    @Override
    public void constraintDrop( ConstraintDescriptor constraint ) throws DropConstraintFailureException
    {
        statement.assertOpen();
        schemaWrite().constraintDrop( statement, constraint );
    }

    // </SchemaWrite>

    // <Locking>
    @Override
    public void acquireExclusive( ResourceType type, long... ids )
    {
        statement.assertOpen();
        locking().acquireExclusive( statement, type, ids );
    }

    @Override
    public void acquireShared( ResourceType type, long... ids )
    {
        statement.assertOpen();
        locking().acquireShared( statement, type, ids );
    }

    @Override
    public void releaseExclusive( ResourceType type, long... ids )
    {
        statement.assertOpen();
        locking().releaseExclusive( statement, type, ids );
    }

    @Override
    public void releaseShared( ResourceType type, long... ids )
    {
        statement.assertOpen();
        locking().releaseShared( statement, type, ids );
    }
    // </Locking>

    // <Explicit index>
    @Override
    public boolean nodeExplicitIndexExists( String indexName, Map<String,String> customConfiguration )
    {
        statement.assertOpen();
        return explicitIndexRead().nodeExplicitIndexExists( statement, indexName, customConfiguration );
    }

    @Override
    public boolean relationshipExplicitIndexExists( String indexName, Map<String,String> customConfiguration )
    {
        statement.assertOpen();
        return explicitIndexRead().relationshipExplicitIndexExists( statement, indexName, customConfiguration );
    }

    @Override
    public ExplicitIndexHits nodeExplicitIndexGet( String indexName, String key, Object value )
            throws ExplicitIndexNotFoundKernelException
    {
        statement.assertOpen();
        return explicitIndexRead().nodeExplicitIndexGet( statement, indexName, key, value );
    }

    @Override
    public ExplicitIndexHits nodeExplicitIndexQuery( String indexName, String key, Object queryOrQueryObject )
            throws ExplicitIndexNotFoundKernelException
    {
        statement.assertOpen();
        return explicitIndexRead().nodeExplicitIndexQuery( statement, indexName, key, queryOrQueryObject );
    }

    @Override
    public ExplicitIndexHits nodeExplicitIndexQuery( String indexName, Object queryOrQueryObject )
            throws ExplicitIndexNotFoundKernelException
    {
        statement.assertOpen();
        return explicitIndexRead().nodeExplicitIndexQuery( statement, indexName, queryOrQueryObject );
    }

    @Override
    public ExplicitIndexHits relationshipExplicitIndexGet( String indexName, String key, Object value,
            long startNode, long endNode ) throws ExplicitIndexNotFoundKernelException
    {
        statement.assertOpen();
        return explicitIndexRead().relationshipExplicitIndexGet( statement, indexName, key, value, startNode, endNode );
    }

    @Override
    public ExplicitIndexHits relationshipExplicitIndexQuery( String indexName, String key, Object queryOrQueryObject,
            long startNode, long endNode ) throws ExplicitIndexNotFoundKernelException
    {
        statement.assertOpen();
        return explicitIndexRead().relationshipExplicitIndexQuery( statement, indexName, key, queryOrQueryObject,
                startNode, endNode );
    }

    @Override
    public ExplicitIndexHits relationshipExplicitIndexQuery( String indexName, Object queryOrQueryObject,
            long startNode, long endNode ) throws ExplicitIndexNotFoundKernelException
    {
        statement.assertOpen();
        return explicitIndexRead().relationshipExplicitIndexQuery( statement, indexName, queryOrQueryObject,
                startNode, endNode );
    }

    @Override
    public void nodeExplicitIndexCreateLazily( String indexName, Map<String,String> customConfig )
    {
        statement.assertOpen();
        explicitIndexWrite().nodeExplicitIndexCreateLazily( statement, indexName, customConfig );
    }

    @Override
    public void nodeExplicitIndexCreate( String indexName, Map<String,String> customConfig )
    {
        statement.assertOpen();

        explicitIndexWrite().nodeExplicitIndexCreate( statement, indexName, customConfig );
    }

    @Override
    public void relationshipExplicitIndexCreateLazily( String indexName, Map<String,String> customConfig )
    {
        statement.assertOpen();
        explicitIndexWrite().relationshipExplicitIndexCreateLazily( statement, indexName, customConfig );
    }

    @Override
    public void relationshipExplicitIndexCreate( String indexName, Map<String,String> customConfig )
    {
        statement.assertOpen();

        explicitIndexWrite().relationshipExplicitIndexCreate( statement, indexName, customConfig );
    }

    @Override
    public void nodeAddToExplicitIndex( String indexName, long node, String key, Object value )
            throws EntityNotFoundException, ExplicitIndexNotFoundKernelException
    {
        statement.assertOpen();
        explicitIndexWrite().nodeAddToExplicitIndex( statement, indexName, node, key, value );
    }

    @Override
    public void nodeRemoveFromExplicitIndex( String indexName, long node, String key, Object value )
            throws ExplicitIndexNotFoundKernelException
    {
        statement.assertOpen();
        explicitIndexWrite().nodeRemoveFromExplicitIndex( statement, indexName, node, key, value );
    }

    @Override
    public void nodeRemoveFromExplicitIndex( String indexName, long node, String key )
            throws ExplicitIndexNotFoundKernelException
    {
        statement.assertOpen();
        explicitIndexWrite().nodeRemoveFromExplicitIndex( statement, indexName, node, key );
    }

    @Override
    public void nodeRemoveFromExplicitIndex( String indexName, long node ) throws ExplicitIndexNotFoundKernelException
    {
        statement.assertOpen();
        explicitIndexWrite().nodeRemoveFromExplicitIndex( statement, indexName, node );
    }

    @Override
    public void relationshipAddToExplicitIndex( String indexName, long relationship, String key, Object value )
            throws EntityNotFoundException, ExplicitIndexNotFoundKernelException
    {
        statement.assertOpen();
        explicitIndexWrite().relationshipAddToExplicitIndex( statement, indexName, relationship, key, value );
    }

    @Override
    public void relationshipRemoveFromExplicitIndex( String indexName, long relationship, String key, Object value )
            throws EntityNotFoundException, ExplicitIndexNotFoundKernelException
    {
        statement.assertOpen();
        explicitIndexWrite().relationshipRemoveFromExplicitIndex( statement, indexName, relationship, key, value );
    }

    @Override
    public void relationshipRemoveFromExplicitIndex( String indexName, long relationship, String key )
            throws ExplicitIndexNotFoundKernelException, EntityNotFoundException
    {
        statement.assertOpen();
        explicitIndexWrite().relationshipRemoveFromExplicitIndex( statement, indexName, relationship, key );
    }

    @Override
    public void relationshipRemoveFromExplicitIndex( String indexName, long relationship )
            throws ExplicitIndexNotFoundKernelException, EntityNotFoundException
    {
        statement.assertOpen();
        explicitIndexWrite().relationshipRemoveFromExplicitIndex( statement, indexName, relationship );
    }

    @Override
    public void nodeExplicitIndexDrop( String indexName ) throws ExplicitIndexNotFoundKernelException
    {
        statement.assertOpen();
        explicitIndexWrite().nodeExplicitIndexDrop( statement, indexName );
    }

    @Override
    public void relationshipExplicitIndexDrop( String indexName ) throws ExplicitIndexNotFoundKernelException
    {
        statement.assertOpen();
        explicitIndexWrite().relationshipExplicitIndexDrop( statement, indexName );
    }

    @Override
    public Map<String,String> nodeExplicitIndexGetConfiguration( String indexName )
            throws ExplicitIndexNotFoundKernelException
    {
        statement.assertOpen();
        return explicitIndexRead().nodeExplicitIndexGetConfiguration( statement, indexName );
    }

    @Override
    public Map<String,String> relationshipExplicitIndexGetConfiguration( String indexName )
            throws ExplicitIndexNotFoundKernelException
    {
        statement.assertOpen();
        return explicitIndexRead().relationshipExplicitIndexGetConfiguration( statement, indexName );
    }

    @Override
    public String nodeExplicitIndexSetConfiguration( String indexName, String key, String value )
            throws ExplicitIndexNotFoundKernelException
    {
        statement.assertOpen();
        return explicitIndexWrite().nodeExplicitIndexSetConfiguration( statement, indexName, key, value );
    }

    @Override
    public String relationshipExplicitIndexSetConfiguration( String indexName, String key, String value )
            throws ExplicitIndexNotFoundKernelException
    {
        statement.assertOpen();
        return explicitIndexWrite().relationshipExplicitIndexSetConfiguration( statement, indexName, key, value );
    }

    @Override
    public String nodeExplicitIndexRemoveConfiguration( String indexName, String key )
            throws ExplicitIndexNotFoundKernelException
    {
        statement.assertOpen();
        return explicitIndexWrite().nodeExplicitIndexRemoveConfiguration( statement, indexName, key );
    }

    @Override
    public String relationshipExplicitIndexRemoveConfiguration( String indexName, String key )
            throws ExplicitIndexNotFoundKernelException
    {
        statement.assertOpen();
        return explicitIndexWrite().relationshipExplicitIndexRemoveConfiguration( statement, indexName, key );
    }

    @Override
    public String[] nodeExplicitIndexesGetAll()
    {
        statement.assertOpen();
        return explicitIndexRead().nodeExplicitIndexesGetAll( statement );
    }

    @Override
    public String[] relationshipExplicitIndexesGetAll()
    {
        statement.assertOpen();
        return explicitIndexRead().relationshipExplicitIndexesGetAll( statement );
    }
    // </Explicit index>

    // <Counts>

    @Override
    public long countsForNode( int labelId )
    {
        statement.assertOpen();
        return counting().countsForNode( statement, labelId );
    }

    @Override
    public long countsForNodeWithoutTxState( int labelId )
    {
        statement.assertOpen();
        return counting().countsForNodeWithoutTxState( statement, labelId );
    }

    @Override
    public long countsForRelationship( int startLabelId, int typeId, int endLabelId )
    {
        statement.assertOpen();
        return counting().countsForRelationship( statement, startLabelId, typeId, endLabelId );
    }

    @Override
    public long countsForRelationshipWithoutTxState( int startLabelId, int typeId, int endLabelId )
    {
        statement.assertOpen();
        return counting().countsForRelationshipWithoutTxState( statement, startLabelId, typeId, endLabelId );
    }

    @Override
    public DoubleLongRegister indexUpdatesAndSize( IndexDescriptor index, DoubleLongRegister target )
            throws IndexNotFoundKernelException
    {
        statement.assertOpen();
        return counting().indexUpdatesAndSize( statement, index, target );
    }

    @Override
    public DoubleLongRegister indexSample( IndexDescriptor index, DoubleLongRegister target )
            throws IndexNotFoundKernelException
    {
        statement.assertOpen();
        return counting().indexSample( statement, index, target );
    }

    // </Counts>

    // query monitoring

    @Override
    public void setMetaData( Map<String,Object> data )
    {
        statement.assertOpen();
        statement.getTransaction().setMetaData( data );
    }

    @Override
    public Map<String,Object> getMetaData()
    {
        statement.assertOpen();
        return statement.getTransaction().getMetaData();
    }

    @Override
    public Stream<ExecutingQuery> executingQueries()
    {
        statement.assertOpen();
        return queryRegistrationOperations().executingQueries( statement );
    }

    @Override
    public ExecutingQuery startQueryExecution(
        ClientConnectionInfo descriptor,
        String queryText,
        MapValue queryParameters )
    {
        statement.assertOpen();
        return queryRegistrationOperations().startQueryExecution( statement, descriptor, queryText, queryParameters );
    }

    @Override
    public void registerExecutingQuery( ExecutingQuery executingQuery )
    {
        statement.assertOpen();
        queryRegistrationOperations().registerExecutingQuery( statement, executingQuery );
    }

    @Override
    public void unregisterExecutingQuery( ExecutingQuery executingQuery )
    {
        queryRegistrationOperations().unregisterExecutingQuery( statement, executingQuery );
    }

    // query monitoring

    // <Procedures>

    @Override
    public RawIterator<Object[],ProcedureException> procedureCallRead( QualifiedName name, Object[] input )
            throws ProcedureException
    {
        AccessMode accessMode = tx.securityContext().mode();
        if ( !accessMode.allowsReads() )
        {
            throw accessMode.onViolation( format( "Read operations are not allowed for %s.",
                    tx.securityContext().description() ) );
        }
        return callProcedure( name, input, new RestrictedAccessMode( tx.securityContext().mode(), AccessMode.Static
                .READ ) );
    }

    @Override
    public RawIterator<Object[],ProcedureException> procedureCallReadOverride( QualifiedName name, Object[] input )
            throws ProcedureException
    {
        return callProcedure( name, input,
                new OverriddenAccessMode( tx.securityContext().mode(), AccessMode.Static.READ ) );
    }

    @Override
    public RawIterator<Object[],ProcedureException> procedureCallWrite( QualifiedName name, Object[] input )
            throws ProcedureException
    {
        AccessMode accessMode = tx.securityContext().mode();
        if ( !accessMode.allowsWrites() )
        {
            throw accessMode.onViolation( format( "Write operations are not allowed for %s.",
                    tx.securityContext().description() ) );
        }
        return callProcedure( name, input, new RestrictedAccessMode( tx.securityContext().mode(), AccessMode.Static.TOKEN_WRITE ) );
    }

    @Override
    public RawIterator<Object[],ProcedureException> procedureCallWriteOverride( QualifiedName name, Object[] input )
            throws ProcedureException
    {
        return callProcedure( name, input, new OverriddenAccessMode( tx.securityContext().mode(), AccessMode.Static.TOKEN_WRITE ) );
    }

    @Override
    public RawIterator<Object[],ProcedureException> procedureCallSchema( QualifiedName name, Object[] input )
            throws ProcedureException
    {
        AccessMode accessMode = tx.securityContext().mode();
        if ( !accessMode.allowsSchemaWrites() )
        {
            throw accessMode.onViolation( format( "Schema operations are not allowed for %s.",
                    tx.securityContext().description() ) );
        }
        return callProcedure( name, input,
                new RestrictedAccessMode( tx.securityContext().mode(), AccessMode.Static.FULL ) );
    }

    @Override
    public RawIterator<Object[],ProcedureException> procedureCallSchemaOverride( QualifiedName name, Object[] input )
            throws ProcedureException
    {
        return callProcedure( name, input,
                new OverriddenAccessMode( tx.securityContext().mode(), AccessMode.Static.FULL ) );
    }

    private RawIterator<Object[],ProcedureException> callProcedure(
            QualifiedName name, Object[] input, final AccessMode override  )
            throws ProcedureException
    {
        statement.assertOpen();

        final SecurityContext procedureSecurityContext = tx.securityContext().withMode( override );
        final RawIterator<Object[],ProcedureException> procedureCall;
        try ( KernelTransaction.Revertable ignore = tx.overrideWith( procedureSecurityContext ) )
        {
            BasicContext ctx = new BasicContext();
            ctx.put( Context.KERNEL_TRANSACTION, tx );
            ctx.put( Context.THREAD, Thread.currentThread() );
            ctx.put( Context.SECURITY_CONTEXT, procedureSecurityContext );
            procedureCall = procedures.callProcedure( ctx, name, input );
        }
        return new RawIterator<Object[],ProcedureException>()
        {
            @Override
            public boolean hasNext() throws ProcedureException
            {
                try ( KernelTransaction.Revertable ignore = tx.overrideWith( procedureSecurityContext ) )
                {
                    return procedureCall.hasNext();
                }
            }

            @Override
            public Object[] next() throws ProcedureException
            {
                try ( KernelTransaction.Revertable ignore = tx.overrideWith( procedureSecurityContext ) )
                {
                    return procedureCall.next();
                }
            }
        };
    }

    @Override
    public Object functionCall( QualifiedName name, Object[] arguments ) throws ProcedureException
    {
        if ( !tx.securityContext().mode().allowsReads() )
        {
            throw tx.securityContext().mode().onViolation(
                    format( "Read operations are not allowed for %s.", tx.securityContext().description() ) );
        }
        return callFunction( name, arguments,
                new RestrictedAccessMode( tx.securityContext().mode(), AccessMode.Static.READ ) );
    }

    @Override
    public Object functionCallOverride( QualifiedName name, Object[] arguments ) throws ProcedureException
    {
        return callFunction( name, arguments,
                new OverriddenAccessMode( tx.securityContext().mode(), AccessMode.Static.READ ) );
    }

    @Override
    public CallableUserAggregationFunction.Aggregator aggregationFunction( QualifiedName name ) throws ProcedureException
    {
        if ( !tx.securityContext().mode().allowsReads() )
        {
            throw tx.securityContext().mode().onViolation(
                    format( "Read operations are not allowed for %s.", tx.securityContext().description() ) );
        }
        return aggregationFunction( name, new RestrictedAccessMode( tx.securityContext().mode(), AccessMode.Static.READ ) );
    }

    @Override
    public CallableUserAggregationFunction.Aggregator aggregationFunctionOverride( QualifiedName name ) throws ProcedureException
    {
        return aggregationFunction( name,
                new OverriddenAccessMode( tx.securityContext().mode(), AccessMode.Static.READ ) );
    }

    private Object callFunction( QualifiedName name, Object[] input, final AccessMode mode ) throws ProcedureException
    {
        statement.assertOpen();

        try ( KernelTransaction.Revertable ignore = tx.overrideWith( tx.securityContext().withMode( mode ) ) )
        {
            BasicContext ctx = new BasicContext();
            ctx.put( Context.KERNEL_TRANSACTION, tx );
            ctx.put( Context.THREAD, Thread.currentThread() );
            return procedures.callFunction( ctx, name, input );
        }
    }

    private CallableUserAggregationFunction.Aggregator aggregationFunction( QualifiedName name, final AccessMode mode )
            throws ProcedureException
    {
        statement.assertOpen();

        try ( KernelTransaction.Revertable ignore = tx.overrideWith( tx.securityContext().withMode( mode ) ) )
        {
            BasicContext ctx = new BasicContext();
            ctx.put( Context.KERNEL_TRANSACTION, tx );
            ctx.put( Context.THREAD, Thread.currentThread() );
            return procedures.createAggregationFunction( ctx, name );
        }
    }

    @Override
    public PageCursorTracer getPageCursorTracer()
    {
        return statement.getPageCursorTracer();
    }
    // </Procedures>
}

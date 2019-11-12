/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.procedure.builtin;

import org.hamcrest.Matcher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.IntSupplier;

import org.neo4j.common.DependencyResolver;
import org.neo4j.common.Edition;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.helpers.collection.MapUtil;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.PopulationProgress;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.SchemaReadCore;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexConfig;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.LabelSchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory;
import org.neo4j.internal.schema.constraints.NodeExistenceConstraintDescriptor;
import org.neo4j.internal.schema.constraints.NodeKeyConstraintDescriptor;
import org.neo4j.internal.schema.constraints.UniquenessConstraintDescriptor;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.procedure.Context;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.util.DefaultValueMapper;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.impl.GlobalProceduresRegistry;
import org.neo4j.token.api.NamedToken;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static java.util.Collections.emptyIterator;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTNode;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTPath;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTRelationship;
import static org.neo4j.internal.schema.SchemaDescriptor.forLabel;
import static org.neo4j.kernel.api.ResourceTracker.EMPTY_RESOURCE_TRACKER;
import static org.neo4j.kernel.api.index.IndexProvider.EMPTY;
import static org.neo4j.kernel.api.procedure.BasicContext.buildContext;

class BuiltInProceduresTest
{
    private final List<IndexDescriptor> indexes = new LinkedList<>();
    private final List<IndexDescriptor> uniqueIndexes = new LinkedList<>();
    private final List<ConstraintDescriptor> constraints = new LinkedList<>();
    private final Map<Integer,String> labels = new HashMap<>();
    private final Map<Integer,String> propKeys = new HashMap<>();
    private final Map<Integer,String> relTypes = new HashMap<>();

    private final Read read = mock( Read.class );
    private final TokenRead tokens = mock( TokenRead.class );
    private final SchemaRead schemaRead = mock( SchemaRead.class );
    private final SchemaReadCore schemaReadCore = mock( SchemaReadCore.class );
    private final InternalTransaction transaction = mock( InternalTransaction.class );
    private final KernelTransaction tx = mock( KernelTransaction.class );
    private final ProcedureCallContext callContext = mock( ProcedureCallContext.class );
    private final DependencyResolver resolver = mock( DependencyResolver.class );
    private final GraphDatabaseAPI graphDatabaseAPI = mock( GraphDatabaseAPI.class );
    private final IndexingService indexingService = mock( IndexingService.class );
    private final Log log = mock( Log.class );

    private final GlobalProceduresRegistry procs = new GlobalProceduresRegistry();

    @BeforeEach
    void setup() throws Exception
    {
        procs.registerComponent( KernelTransaction.class, ctx -> ctx.internalTransaction().kernelTransaction(), false );
        procs.registerComponent( DependencyResolver.class, Context::dependencyResolver, false );
        procs.registerComponent( GraphDatabaseAPI.class, Context::graphDatabaseAPI, false );
        procs.registerComponent( Transaction.class, Context::internalTransaction, true );
        procs.registerComponent( SecurityContext.class, Context::securityContext, true );
        procs.registerComponent( ProcedureCallContext.class, Context::procedureCallContext, true );

        procs.registerComponent( Log.class, ctx -> log, false );
        procs.registerType( Node.class, NTNode );
        procs.registerType( Relationship.class, NTRelationship );
        procs.registerType( Path.class, NTPath );

        new SpecialBuiltInProcedures( "1.3.37", Edition.COMMUNITY.toString() ).accept( procs );
        procs.registerProcedure( BuiltInProcedures.class );
        procs.registerProcedure( BuiltInDbmsProcedures.class );

        when( transaction.kernelTransaction() ).thenReturn( tx );
        when( tx.tokenRead() ).thenReturn( tokens );
        when( tx.dataRead() ).thenReturn( read );
        when( tx.schemaRead() ).thenReturn( schemaRead );
        when( tx.securityContext() ).thenReturn( SecurityContext.AUTH_DISABLED );
        when( callContext.isCalledFromCypher() ).thenReturn( false );
        when( schemaRead.snapshot() ).thenReturn( schemaReadCore );

        when( tokens.propertyKeyGetAllTokens() ).thenAnswer( asTokens( propKeys ) );
        when( tokens.labelsGetAllTokens() ).thenAnswer( asTokens( labels ) );
        when( tokens.relationshipTypesGetAllTokens() ).thenAnswer( asTokens( relTypes ) );
        when( schemaReadCore.indexesGetAll() ).thenAnswer(
                i -> Iterators.concat( indexes.iterator(), uniqueIndexes.iterator() ) );
        when( schemaReadCore.index( any( SchemaDescriptor.class ) ) ).thenAnswer( (Answer<IndexDescriptor>) invocationOnMock -> {
            SchemaDescriptor schema = invocationOnMock.getArgument( 0 );
            return getIndexReference( schema );
        } );
        when( schemaReadCore.constraintsGetAll() ).thenAnswer( i -> constraints.iterator() );

        when( tokens.propertyKeyName( anyInt() ) ).thenAnswer( invocation -> propKeys.get( invocation.getArgument( 0 ) ) );
        when( tokens.nodeLabelName( anyInt() ) ).thenAnswer( invocation -> labels.get( invocation.getArgument( 0 ) ) );
        when( tokens.relationshipTypeName( anyInt() ) ).thenAnswer( invocation -> relTypes.get( invocation.getArgument( 0 ) ) );

        when( schemaReadCore.constraintsGetForRelationshipType( anyInt() ) ).thenReturn( emptyIterator() );
        when( schemaReadCore.indexesGetForLabel( anyInt() ) ).thenReturn( emptyIterator() );
        when( schemaReadCore.indexesGetForRelationshipType( anyInt() ) ).thenReturn( emptyIterator() );
        when( schemaReadCore.constraintsGetForLabel( anyInt() ) ).thenReturn( emptyIterator() );
        when( read.countsForNode( anyInt() ) ).thenReturn( 1L );
        when( read.countsForRelationship( anyInt(), anyInt(), anyInt() ) ).thenReturn( 1L );
        when( schemaReadCore.indexGetState( any( IndexDescriptor.class ) ) ).thenReturn( InternalIndexState.ONLINE );
    }

    @Test
    void shouldListAllIndexes() throws Throwable
    {
        // Given
        givenIndex( "User", "name" );

        // When/Then
        assertThat( call( "db.indexes" ), contains( record(
                1000L,
                "index_1000",
                "ONLINE",
                100D,
                "NONUNIQUE",
                "BTREE",
                "NODE",
                singletonList( "User" ),
                singletonList( "name" ),
                "no-index-provider-1.0" )
        ) );
    }

    @Test
    void lookupComponentProviders()
    {
        assertNotNull( procs.lookupComponentProvider( Transaction.class, true ) );
        assertNotNull( procs.lookupComponentProvider( Transaction.class, false ) );

        assertNull( procs.lookupComponentProvider( Statement.class, true ) );
        assertNull( procs.lookupComponentProvider( Statement.class, false ) );

        assertNull( procs.lookupComponentProvider( DependencyResolver.class, true ) );
        assertNotNull( procs.lookupComponentProvider( DependencyResolver.class, false ) );
    }

    @Test
    void shouldListAllUniqueIndexes() throws Throwable
    {
        // Given
        givenUniqueConstraint( "User", "name" );

        // When/Then
        assertThat( call( "db.indexes" ), contains( record(
                1000L,
                "constraint_1000",
                "ONLINE",
                100D,
                "UNIQUE",
                "BTREE",
                "NODE",
                singletonList( "User" ),
                singletonList( "name" ),
                "no-index-provider-1.0" )
        ) );
    }

    @Test
    void indexDetailsShouldGiveDetailedIndexInfo() throws Throwable
    {
        // Given
        givenIndex( "User", "name" );

        // When/Then
        final Map<String,Object> configMap = MapUtil.genericMap( new HashMap<>(), "config1", "value1", "config2", 2, "config3", true );
        assertThat( call( "db.indexDetails", "index_" + 1000 ), contains( record(
                1000L, "index_1000", "ONLINE", 100D, "NONUNIQUE", "BTREE", "NODE", singletonList( "User" ), singletonList( "name" ),
                EMPTY.getProviderDescriptor().name(), configMap, "" ) ) );
    }

    @Test
    void indexDetailsShouldGiveMessageForConcurrentlyDeletedIndexes() throws Throwable
    {
        // Given
        givenIndex( "User", "name" );
        when( schemaReadCore.indexGetState( any( IndexDescriptor.class ) ) ).thenThrow( new IndexNotFoundKernelException( "Not found." ) );

        // When/Then
        final Map<String,Object> configMap = MapUtil.genericMap( new HashMap<>(), "config1", "value1", "config2", 2, "config3", true );
        assertThat( call( "db.indexDetails", "index_" + 1000 ), contains( record(
                1000L, "index_1000", "NOT FOUND", 0D, "NONUNIQUE", "BTREE", "NODE", singletonList( "User" ), singletonList( "name" ),
                EMPTY.getProviderDescriptor().name(), configMap, "Index not found. It might have been concurrently dropped." ) ) );
    }

    @Test
    void shouldListPropertyKeys() throws Throwable
    {
        // Given
        givenPropertyKeys( "name", "age" );

        // When/Then
        assertThat( call( "db.propertyKeys" ),
                containsInAnyOrder(
                        record( "age" ),
                        record( "name" ) ) );
    }

    @Test
    void shouldListLabels() throws Throwable
    {
        // Given
        givenLabels( "Banana", "Fruit" );

        // When/Then
        assertThat( call( "db.labels" ),
                containsInAnyOrder(
                        record( "Banana" ),
                        record( "Fruit" ) ) );
    }

    @Test
    void shouldListRelTypes() throws Throwable
    {
        // Given
        givenRelationshipTypes( "EATS", "SPROUTS" );

        // When/Then
        assertThat( call( "db.relationshipTypes" ),
                containsInAnyOrder(
                        record( "EATS" ),
                        record( "SPROUTS" ) ) );
    }

    @Test
    void shouldListConstraints() throws Throwable
    {
        // Given
        givenUniqueConstraint( "User", "name" );
        givenNodePropExistenceConstraint( "User", "name" );
        givenNodeKeys( "User", "name" );
        // When/Then
        assertThat( call( "db.constraints" ),
                contains(
                        record( "MyExistenceConstraint", "CONSTRAINT ON ( user:User ) ASSERT exists(user.name)" ),
                        record( "MyNodeKeyConstraint", "CONSTRAINT ON ( user:User ) ASSERT (user.name) IS NODE KEY" ),
                        record( "constraint_1000", "CONSTRAINT ON ( user:User ) ASSERT (user.name) IS UNIQUE" )
                ) );
    }

    @Test
    void shouldEscapeLabelNameContainingColons() throws Throwable
    {
        // Given
        givenUniqueConstraint( "FOO:BAR", "x.y" );
        givenNodePropExistenceConstraint( "FOO:BAR", "x.y" );

        // When/Then
        List<Object[]> call = call( "db.constraints" );
        assertThat( call,
                contains(
                        record( "MyExistenceConstraint", "CONSTRAINT ON ( `foo:bar`:`FOO:BAR` ) ASSERT exists(`foo:bar`.x.y)" ),
                        record( "constraint_1000", "CONSTRAINT ON ( `foo:bar`:`FOO:BAR` ) ASSERT (`foo:bar`.x.y) IS UNIQUE" ) ) );
    }

    @Test
    void shouldListSystemComponents() throws Throwable
    {
        // When/Then
        assertThat( call( "dbms.components" ), contains(
                record( "Neo4j Kernel", singletonList( "1.3.37" ), "community" )
        ) );
    }

    @Test
    void shouldCloseStatementIfExceptionIsThrownDbLabels()
    {
        // Given
        RuntimeException runtimeException = new RuntimeException();
        when( tokens.labelsGetAllTokens() ).thenThrow( runtimeException );

        // When
        assertThrows( ProcedureException.class, () -> call( "db.labels" ) );

    }

    @Test
    void shouldCloseStatementIfExceptionIsThrownDbPropertyKeys()
    {
        // Given
        RuntimeException runtimeException = new RuntimeException();
        when( tokens.propertyKeyGetAllTokens() ).thenThrow( runtimeException );

        // When
        assertThrows( ProcedureException.class, () -> call( "db.propertyKeys" ) );

    }

    @Test
    void shouldCloseStatementIfExceptionIsThrownDbRelationshipTypes()
    {
        // Given
        RuntimeException runtimeException = new RuntimeException();
        when( tokens.relationshipTypesGetAllTokens() ).thenThrow( runtimeException );

        // When
        assertThrows( ProcedureException.class, () -> call( "db.relationshipTypes" ) );

    }

    private static Matcher<Object[]> record( Object... fields )
    {
        return equalTo( fields );
    }

    private void givenIndex( String label, String propKey )
    {
        int labelId = token( label, labels );
        int propId = token( propKey, propKeys );

        int id = indexes.size() + 1000;
        Map<String,Value> configMap = new HashMap<>();
        configMap.put( "config1", Values.stringValue( "value1" ) );
        configMap.put( "config2", Values.intValue( 2 ) );
        configMap.put( "config3", Values.booleanValue( true ) );
        LabelSchemaDescriptor schema = forLabel( labelId, propId );
        IndexDescriptor index = IndexPrototype.forSchema( schema, EMPTY.getProviderDescriptor() )
                .withName( "index_" + id )
                .materialise( id )
                .withIndexConfig( IndexConfig.with( configMap ) );
        indexes.add( index );
    }

    private void givenUniqueConstraint( String label, String propKey )
    {
        int labelId = token( label, labels );
        int propId = token( propKey, propKeys );

        LabelSchemaDescriptor schema = forLabel( labelId, propId );
        int id = uniqueIndexes.size() + 1000;
        final String name = "constraint_" + id;
        IndexDescriptor index = IndexPrototype.uniqueForSchema( schema, EMPTY.getProviderDescriptor() )
                .withName( name )
                .materialise( id );
        uniqueIndexes.add( index );
        final UniquenessConstraintDescriptor constraint = ConstraintDescriptorFactory.uniqueForLabel( labelId, propId )
                .withName( name );
        constraints.add( constraint );
    }

    private void givenNodePropExistenceConstraint( String label, String propKey )
    {
        int labelId = token( label, labels );
        int propId = token( propKey, propKeys );

        final NodeExistenceConstraintDescriptor constraint = ConstraintDescriptorFactory.existsForLabel( labelId, propId )
                .withName( "MyExistenceConstraint" );
        constraints.add( constraint );
    }

    private void givenNodeKeys( String label, String...props )
    {
        int labelId = token( label, labels );
        int[] propIds = new int[props.length];
        for ( int i = 0; i < propIds.length; i++ )
        {
            propIds[i] = token( props[i], propKeys );
        }
        final NodeKeyConstraintDescriptor constraint = ConstraintDescriptorFactory.nodeKeyForLabel( labelId, propIds )
                .withName( "MyNodeKeyConstraint" );
        constraints.add( constraint );
    }

    private void givenPropertyKeys( String... keys )
    {
        for ( String key : keys )
        {
            token( key, propKeys );
        }
    }

    private void givenLabels( String... labelNames )
    {
        for ( String key : labelNames )
        {
            token( key, labels );
        }
    }

    private void givenRelationshipTypes( String... types )
    {
        for ( String key : types )
        {
            token( key, relTypes );
        }
    }

    private static Integer token( String name, Map<Integer,String> tokens )
    {
        IntSupplier allocateFromMap = () ->
        {
            int newIndex = tokens.size();
            tokens.put( newIndex, name );
            return newIndex;
        };
        return tokens.entrySet().stream()
                     .filter( entry -> entry.getValue().equals( name ) )
                     .mapToInt( Map.Entry::getKey )
                     .findFirst().orElseGet( allocateFromMap );
    }

    private IndexDescriptor getIndexReference( SchemaDescriptor schema )
    {
        for ( IndexDescriptor index : indexes )
        {
            if ( index.schema().equals( schema ) )
            {
                return index;
            }
        }
        for ( IndexDescriptor index : uniqueIndexes )
        {
            if ( index.schema().equals( schema ) )
            {
                return index;
            }
        }
        throw new AssertionError( "No index matching the schema: " + schema );
    }

    private static Answer<Iterator<NamedToken>> asTokens( Map<Integer,String> tokens )
    {
        return i -> tokens.entrySet().stream()
                              .map( entry -> new NamedToken( entry.getValue(), entry.getKey() ) )
                              .iterator();
    }

    private List<Object[]> call( String name, Object... args ) throws ProcedureException, IndexNotFoundKernelException
    {
        DefaultValueMapper valueMapper = new DefaultValueMapper( mock( InternalTransaction.class ) );
        Context ctx = buildContext(resolver, valueMapper )
                        .withTransaction( transaction )
                        .withProcedureCallContext( callContext )
                        .context();

        when( graphDatabaseAPI.getDependencyResolver() ).thenReturn( resolver );
        when( resolver.resolveDependency( GraphDatabaseAPI.class ) ).thenReturn( graphDatabaseAPI );
        when( resolver.resolveDependency( GlobalProceduresRegistry.class ) ).thenReturn( procs );
        when( resolver.resolveDependency( IndexingService.class ) ).thenReturn( indexingService );
        when( schemaReadCore.indexGetPopulationProgress( any( IndexDescriptor.class) ) ).thenReturn( PopulationProgress.DONE );
        AnyValue[] input = Arrays.stream( args ).map( ValueUtils::of ).toArray( AnyValue[]::new );
        int procId = procs.procedure( ProcedureSignature.procedureName( name.split( "\\." ) ) ).id();
        List<AnyValue[]> anyValues =
                Iterators.asList( procs.callProcedure( ctx, procId, input, EMPTY_RESOURCE_TRACKER ) );
        ArrayList<Object[]> toReturn = new ArrayList<>( anyValues.size() );
        for ( AnyValue[] anyValue : anyValues )
        {
            Object[] values = new Object[anyValue.length];
            for ( int i = 0; i < anyValue.length; i++ )
            {
                AnyValue value = anyValue[i];
                values[i] = value.map( valueMapper );
            }
            toReturn.add( values );
        }
        return toReturn;
    }
}

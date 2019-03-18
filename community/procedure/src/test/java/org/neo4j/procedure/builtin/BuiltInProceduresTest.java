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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.IntSupplier;
import java.util.stream.Collectors;

import org.neo4j.common.DependencyResolver;
import org.neo4j.common.Edition;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.internal.kernel.api.IndexReference;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.PopulationProgress;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.constraints.AbstractConstraintDescriptor;
import org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.index.IndexProviderDescriptor;
import org.neo4j.kernel.api.procedure.Context;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.core.EmbeddedProxySPI;
import org.neo4j.kernel.impl.index.schema.IndexDescriptorFactory;
import org.neo4j.kernel.impl.util.DefaultValueMapper;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.impl.GlobalProceduresRegistry;
import org.neo4j.token.api.NamedToken;
import org.neo4j.values.AnyValue;

import static java.util.Collections.emptyIterator;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTNode;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTPath;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTRelationship;
import static org.neo4j.internal.schema.SchemaDescriptorFactory.forLabel;
import static org.neo4j.kernel.api.ResourceManager.EMPTY_RESOURCE_MANAGER;
import static org.neo4j.kernel.api.index.IndexProvider.EMPTY;
import static org.neo4j.kernel.api.procedure.BasicContext.buildContext;

class BuiltInProceduresTest
{
    private final List<IndexReference> indexes = new LinkedList<>();
    private final List<IndexReference> uniqueIndexes = new LinkedList<>();
    private final List<AbstractConstraintDescriptor> constraints = new LinkedList<>();
    private final Map<Integer,String> labels = new HashMap<>();
    private final Map<Integer,String> propKeys = new HashMap<>();
    private final Map<Integer,String> relTypes = new HashMap<>();

    private final Read read = mock( Read.class );
    private final TokenRead tokens = mock( TokenRead.class );
    private final SchemaRead schemaRead = mock( SchemaRead.class );
    private final Statement statement = mock( Statement.class );
    private final KernelTransaction tx = mock( KernelTransaction.class );
    private final DependencyResolver resolver = mock( DependencyResolver.class );
    private final GraphDatabaseAPI graphDatabaseAPI = mock( GraphDatabaseAPI.class );
    private final IndexingService indexingService = mock( IndexingService.class );
    private final Log log = mock( Log.class );

    private final GlobalProceduresRegistry procs = new GlobalProceduresRegistry();

    @BeforeEach
    void setup() throws Exception
    {
        procs.registerComponent( KernelTransaction.class, Context::kernelTransaction, false );
        procs.registerComponent( DependencyResolver.class, Context::dependencyResolver, false );
        procs.registerComponent( GraphDatabaseAPI.class, Context::graphDatabaseAPI, false );
        procs.registerComponent( SecurityContext.class, ctx -> ctx.securityContext(), true );

        procs.registerComponent( Log.class, ctx -> log, false );
        procs.registerType( Node.class, NTNode );
        procs.registerType( Relationship.class, NTRelationship );
        procs.registerType( Path.class, NTPath );

        new SpecialBuiltInProcedures( "1.3.37", Edition.COMMUNITY.toString() ).accept( procs );
        procs.registerProcedure( BuiltInProcedures.class );
        procs.registerProcedure( BuiltInDbmsProcedures.class );

        when( tx.acquireStatement() ).thenReturn( statement );
        when( tx.tokenRead() ).thenReturn( tokens );
        when( tx.dataRead() ).thenReturn( read );
        when( tx.schemaRead() ).thenReturn( schemaRead );

        when( tokens.propertyKeyGetAllTokens() ).thenAnswer( asTokens( propKeys ) );
        when( tokens.labelsGetAllTokens() ).thenAnswer( asTokens( labels ) );
        when( tokens.relationshipTypesGetAllTokens() ).thenAnswer( asTokens( relTypes ) );
        when( schemaRead.indexesGetAll() ).thenAnswer(
                i -> Iterators.concat( indexes.iterator(), uniqueIndexes.iterator() ) );
        when( schemaRead.index( any( SchemaDescriptor.class ) ) ).thenAnswer( (Answer<IndexReference>) invocationOnMock -> {
            SchemaDescriptor schema = invocationOnMock.getArgument( 0 );
            int label = schema.keyId();
            int prop = schema.getPropertyId();
            return getIndexReference( label, prop );
        } );
        when( schemaRead.constraintsGetAll() ).thenAnswer( i -> constraints.iterator() );

        when( tokens.propertyKeyName( anyInt() ) ).thenAnswer( invocation -> propKeys.get( invocation.getArgument( 0 ) ) );
        when( tokens.nodeLabelName( anyInt() ) ).thenAnswer( invocation -> labels.get( invocation.getArgument( 0 ) ) );
        when( tokens.relationshipTypeName( anyInt() ) ).thenAnswer( invocation -> relTypes.get( invocation.getArgument( 0 ) ) );

        when( indexingService.getIndexId( any( SchemaDescriptor.class ) ) ).thenReturn( 42L );

        when( schemaRead.constraintsGetForRelationshipType( anyInt() ) ).thenReturn( emptyIterator() );
        when( schemaRead.indexesGetForLabel( anyInt() ) ).thenReturn( emptyIterator() );
        when( schemaRead.constraintsGetForLabel( anyInt() ) ).thenReturn( emptyIterator() );
        when( read.countsForNode( anyInt() ) ).thenReturn( 1L );
        when( read.countsForRelationship( anyInt(), anyInt(), anyInt() ) ).thenReturn( 1L );
        when( schemaRead.indexGetState( any( IndexReference.class ) ) ).thenReturn( InternalIndexState.ONLINE );
    }

    @Test
    void shouldListAllIndexes() throws Throwable
    {
        // Given
        givenIndex( "User", "name" );

        // When/Then
        assertThat( call( "db.indexes" ), contains( record(
                "INDEX ON :User(name)", "Unnamed index", singletonList( "User" ), singletonList( "name" ), "ONLINE", "node_label_property", 100D,
                getIndexProviderDescriptorMap( EMPTY.getProviderDescriptor() ), 42L, "" ) ) );
    }

    @Test
    void shouldListAllUniqueIndexes() throws Throwable
    {
        // Given
        givenUniqueConstraint( "User", "name" );

        // When/Then
        assertThat( call( "db.indexes" ), contains( record(
                "INDEX ON :User(name)", "Unnamed index", singletonList( "User" ), singletonList( "name" ), "ONLINE", "node_unique_property", 100D,
                getIndexProviderDescriptorMap( EMPTY.getProviderDescriptor() ), 42L, "" ) ) );
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
                        record( "Banana", 1L ),
                        record( "Fruit", 1L ) ) );
    }

    @Test
    void shouldListRelTypes() throws Throwable
    {
        // Given
        givenRelationshipTypes( "EATS", "SPROUTS" );

        // When/Then
        assertThat( call( "db.relationshipTypes" ),
                containsInAnyOrder(
                        record( "EATS", 1L ),
                        record( "SPROUTS", 1L ) ) );
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
                containsInAnyOrder(
                        record( "CONSTRAINT ON ( user:User ) ASSERT exists(user.name)" ),
                        record( "CONSTRAINT ON ( user:User ) ASSERT user.name IS UNIQUE" ),
                        record( "CONSTRAINT ON ( user:User ) ASSERT (user.name) IS NODE KEY" )
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
                        record( "CONSTRAINT ON ( `foo:bar`:`FOO:BAR` ) ASSERT `foo:bar`.x.y IS UNIQUE" ),
                        record( "CONSTRAINT ON ( `foo:bar`:`FOO:BAR` ) ASSERT exists(`foo:bar`.x.y)" ) ) );
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

        verify( statement ).close();
    }

    @Test
    void shouldCloseStatementIfExceptionIsThrownDbPropertyKeys()
    {
        // Given
        RuntimeException runtimeException = new RuntimeException();
        when( tokens.propertyKeyGetAllTokens() ).thenThrow( runtimeException );

        // When
        assertThrows( ProcedureException.class, () -> call( "db.propertyKeys" ) );

        verify( statement ).close();
    }

    @Test
    void shouldCloseStatementIfExceptionIsThrownDbRelationshipTypes()
    {
        // Given
        RuntimeException runtimeException = new RuntimeException();
        when( tokens.relationshipTypesGetAllTokens() ).thenThrow( runtimeException );

        // When
        assertThrows( ProcedureException.class, () -> call( "db.relationshipTypes" ) );

        verify( statement ).close();
    }

    private static Map<String,String> getIndexProviderDescriptorMap( IndexProviderDescriptor providerDescriptor )
    {
        return MapUtil.stringMap( "key", providerDescriptor.getKey(), "version", providerDescriptor.getVersion() );
    }

    private static Matcher<Object[]> record( Object... fields )
    {
        return equalTo( fields );
    }

    private void givenIndex( String label, String propKey )
    {
        int labelId = token( label, labels );
        int propId = token( propKey, propKeys );

        IndexReference index = IndexDescriptorFactory.forSchema( forLabel( labelId, propId ), EMPTY.getProviderDescriptor() );
        indexes.add( index );
    }

    private void givenUniqueConstraint( String label, String propKey )
    {
        int labelId = token( label, labels );
        int propId = token( propKey, propKeys );

        IndexReference index = IndexDescriptorFactory.uniqueForSchema( forLabel( labelId, propId ), EMPTY.getProviderDescriptor() );
        uniqueIndexes.add( index );
        constraints.add( ConstraintDescriptorFactory.uniqueForLabel( labelId, propId ) );
    }

    private void givenNodePropExistenceConstraint( String label, String propKey )
    {
        int labelId = token( label, labels );
        int propId = token( propKey, propKeys );

        constraints.add( ConstraintDescriptorFactory.existsForLabel( labelId, propId ) );
    }

    private void givenNodeKeys( String label, String...props )
    {
        int labelId = token( label, labels );
        int[] propIds = new int[props.length];
        for ( int i = 0; i < propIds.length; i++ )
        {
            propIds[i] = token( props[i], propKeys );
        }

        constraints.add( ConstraintDescriptorFactory.nodeKeyForLabel( labelId, propIds ) );
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

    private IndexReference getIndexReference( int label, int prop )
    {
        for ( IndexReference index : indexes )
        {
            if ( index.schema().getEntityTokenIds()[0] == label && prop == index.properties()[0] )
            {
                return index;
            }
        }
        for ( IndexReference index : uniqueIndexes )
        {
            if ( index.schema().getEntityTokenIds()[0] == label && prop == index.properties()[0] )
            {
                return index;
            }
        }
        throw new AssertionError(  );
    }

    private static Answer<Iterator<NamedToken>> asTokens( Map<Integer,String> tokens )
    {
        return i -> tokens.entrySet().stream()
                              .map( entry -> new NamedToken( entry.getValue(), entry.getKey() ) )
                              .iterator();
    }

    private List<Object[]> call( String name, Object... args ) throws ProcedureException, IndexNotFoundKernelException
    {
        DefaultValueMapper valueMapper = new DefaultValueMapper( mock( EmbeddedProxySPI.class ) );
        Context ctx = buildContext(resolver, valueMapper )
                        .withKernelTransaction( tx )
                        .context();

        when( graphDatabaseAPI.getDependencyResolver() ).thenReturn( resolver );
        when( resolver.resolveDependency( GraphDatabaseAPI.class ) ).thenReturn( graphDatabaseAPI );
        when( resolver.resolveDependency( GlobalProceduresRegistry.class ) ).thenReturn( procs );
        when( resolver.resolveDependency( IndexingService.class ) ).thenReturn( indexingService );
        when( schemaRead.indexGetPopulationProgress( any( IndexReference.class) ) ).thenReturn( PopulationProgress.DONE );
        AnyValue[] input = Arrays.stream( args ).map( ValueUtils::of ).toArray( AnyValue[]::new );
        int procId = procs.procedure( ProcedureSignature.procedureName( name.split( "\\." ) ) ).id();
        List<AnyValue[]> anyValues = Iterators.asList( procs.callProcedure( ctx, procId, input, EMPTY_RESOURCE_MANAGER ) );

        return anyValues.stream().map( vs -> Arrays.stream( vs ).map( v -> v.map( valueMapper ) ).toArray( Object[]::new ) )
                .collect( Collectors.toList());

    }
}

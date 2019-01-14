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
package org.neo4j.kernel.builtinprocs;

import org.hamcrest.Matcher;
import org.junit.Before;
import org.junit.Test;
import org.mockito.stubbing.Answer;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.IntSupplier;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.internal.kernel.api.CapableIndexReference;
import org.neo4j.internal.kernel.api.IndexReference;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.NamedToken;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.ResourceTracker;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.StubResourceManager;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.proc.BasicContext;
import org.neo4j.kernel.api.proc.Key;
import org.neo4j.kernel.api.schema.constaints.ConstraintDescriptor;
import org.neo4j.kernel.api.schema.constaints.ConstraintDescriptorFactory;
import org.neo4j.kernel.impl.api.index.inmemory.InMemoryIndexProviderFactory;
import org.neo4j.kernel.impl.api.store.DefaultCapableIndexReference;
import org.neo4j.kernel.impl.api.store.DefaultIndexReference;
import org.neo4j.kernel.impl.factory.Edition;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;

import static java.util.Collections.emptyIterator;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTNode;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTPath;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTRelationship;
import static org.neo4j.kernel.api.proc.Context.KERNEL_TRANSACTION;
import static org.neo4j.kernel.api.proc.Context.SECURITY_CONTEXT;

public class BuiltInProceduresTest
{
    private final List<IndexReference> indexes = new LinkedList<>();
    private final List<IndexReference> uniqueIndexes = new LinkedList<>();
    private final List<ConstraintDescriptor> constraints = new LinkedList<>();
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
    private final Log log = mock( Log.class );

    private final Procedures procs = new Procedures();
    private final ResourceTracker resourceTracker = new StubResourceManager();

    @Test
    public void shouldListAllIndexes() throws Throwable
    {
        // Given
        givenIndex( "User", "name" );

        // When/Then
        assertThat( call( "db.indexes" ),
                contains( record( "INDEX ON :User(name)", "User", singletonList( "name" ), "ONLINE", "node_label_property",
                        getIndexProviderDescriptorMap( InMemoryIndexProviderFactory.PROVIDER_DESCRIPTOR ), "" ) ) );
    }

    private Map<String,String> getIndexProviderDescriptorMap( IndexProvider.Descriptor providerDescriptor )
    {
        return MapUtil.stringMap( "key", providerDescriptor.getKey(), "version", providerDescriptor.getVersion() );
    }

    @Test
    public void shouldListAllUniqueIndexes() throws Throwable
    {
        // Given
        givenUniqueConstraint( "User", "name" );

        // When/Then
        assertThat( call( "db.indexes" ),
                contains( record( "INDEX ON :User(name)", "User", singletonList( "name" ), "ONLINE", "node_unique_property",
                        getIndexProviderDescriptorMap( InMemoryIndexProviderFactory.PROVIDER_DESCRIPTOR ), "" ) ) );
    }

    @Test
    public void shouldListPropertyKeys() throws Throwable
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
    public void shouldListLabels() throws Throwable
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
    public void shouldListRelTypes() throws Throwable
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
    public void shouldListConstraints() throws Throwable
    {
        // Given
        givenUniqueConstraint( "User", "name" );
        givenNodePropExistenceConstraint( "User", "name" );

        // When/Then
        assertThat( call( "db.constraints" ),
                contains(
                        record( "CONSTRAINT ON ( user:User ) ASSERT exists(user.name)" ),
                        record( "CONSTRAINT ON ( user:User ) ASSERT user.name IS UNIQUE" ) ) );
    }

    @Test
    public void shouldEscapeLabelNameContainingColons() throws Throwable
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
    public void shouldListCorrectBuiltinProcedures() throws Throwable
    {
        // When/Then
        assertThat( call( "dbms.procedures" ), containsInAnyOrder(
                record( "dbms.listConfig",
                        "dbms.listConfig(searchString =  :: STRING?) :: (name :: STRING?, description :: STRING?, " +
                        "value" +
                        " :: STRING?)",
                        "List the currently active config of Neo4j.", "DBMS" ),
                record( "db.awaitIndex", "db.awaitIndex(index :: STRING?, timeOutSeconds = 300 :: INTEGER?) :: VOID",
                        "Wait for an index to come online (for example: CALL db.awaitIndex(\":Person(name)\")).", "READ" ),
                record( "db.awaitIndexes", "db.awaitIndexes(timeOutSeconds = 300 :: INTEGER?) :: VOID",
                        "Wait for all indexes to come online (for example: CALL db.awaitIndexes(\"500\")).", "READ" ),
                record( "db.constraints", "db.constraints() :: (description :: STRING?)",
                        "List all constraints in the database.", "READ" ),
                record( "db.indexes", "db.indexes() :: (description :: STRING?, label :: STRING?, properties :: LIST? OF STRING?, " +
                                "state :: STRING?, type :: STRING?, provider :: MAP?, failureMessage :: STRING?)",
                        "List all indexes in the database.", "READ" ),
                record( "db.labels", "db.labels() :: (label :: STRING?)", "List all labels in the database.", "READ" ),
                record( "db.propertyKeys", "db.propertyKeys() :: (propertyKey :: STRING?)",
                        "List all property keys in the database.", "READ" ),
                record( "db.relationshipTypes", "db.relationshipTypes() :: (relationshipType :: STRING?)",
                        "List all relationship types in the database.", "READ" ),
                record( "db.resampleIndex", "db.resampleIndex(index :: STRING?) :: VOID",
                        "Schedule resampling of an index (for example: CALL db.resampleIndex(\":Person(name)\")).", "READ" ),
                record( "db.resampleOutdatedIndexes", "db.resampleOutdatedIndexes() :: VOID",
                        "Schedule resampling of all outdated indexes.", "READ" ),
                record( "db.schema.nodeTypeProperties",
                        "db.schema.nodeTypeProperties() :: (nodeType :: STRING?, nodeLabels :: LIST? OF STRING?, propertyName :: STRING?, " +
                                "propertyTypes :: LIST? OF STRING?, mandatory :: BOOLEAN?)",
                        "Show the derived property schema of the nodes in tabular form.", "READ" ),
                record( "db.schema.relTypeProperties",
                        "db.schema.relTypeProperties() :: (relType :: STRING?, propertyName :: STRING?, propertyTypes :: LIST? OF STRING?," +
                                " mandatory :: BOOLEAN?)",
                        "Show the derived property schema of the relationships in tabular form.", "READ" ),
                record( "db.schema",
                        "db.schema() :: (nodes :: LIST? OF NODE?, relationships :: LIST? OF RELATIONSHIP?)",
                        "Show the schema of the data.", "READ" ),
                record( "db.index.explicit.searchNodes",
                        "db.index.explicit.searchNodes(indexName :: STRING?, query :: ANY?) :: (node :: NODE?, weight :: FLOAT?)",
                        "Search nodes in explicit index. Replaces `START n=node:nodes('key:foo*')`", "READ" ),
                record( "db.index.explicit.seekNodes",
                        "db.index.explicit.seekNodes(indexName :: STRING?, key :: STRING?, value :: ANY?) :: (node :: NODE?)",
                        "Get node from explicit index. Replaces `START n=node:nodes(key = 'A')`", "READ" ),
                record( "db.index.explicit.searchRelationships",
                        "db.index.explicit.searchRelationships(indexName :: STRING?, query :: ANY?) :: " +
                                "(relationship :: RELATIONSHIP?, weight :: FLOAT?)",
                        "Search relationship in explicit index. Replaces `START r=relationship:relIndex('key:foo*')`", "READ" ),
                record( "db.index.explicit.searchRelationshipsIn",
                        "db.index.explicit.searchRelationshipsIn(indexName :: STRING?, in :: NODE?, query :: ANY?) :: " +
                                "(relationship :: RELATIONSHIP?, weight :: FLOAT?)",
                        "Search relationship in explicit index, starting at the node 'in'.", "READ" ),
                record( "db.index.explicit.searchRelationshipsOut",
                        "db.index.explicit.searchRelationshipsOut(indexName :: STRING?, out :: NODE?, query :: ANY?) :: " +
                                "(relationship :: RELATIONSHIP?, weight :: FLOAT?)",
                        "Search relationship in explicit index, ending at the node 'out'.", "READ" ),
                record( "db.index.explicit.searchRelationshipsBetween",
                        "db.index.explicit.searchRelationshipsBetween(indexName :: STRING?, in :: NODE?, out :: NODE?, query :: ANY?) :: " +
                                "(relationship :: RELATIONSHIP?, weight :: FLOAT?)",
                        "Search relationship in explicit index, starting at the node 'in' and ending at 'out'.", "READ" ),
                record( "db.index.explicit.seekRelationships",
                        "db.index.explicit.seekRelationships(indexName :: STRING?, key :: STRING?, value :: ANY?) :: " +
                        "(relationship :: RELATIONSHIP?)",
                        "Get relationship from explicit index. Replaces `START r=relationship:relIndex(key = 'A')`", "READ" ),
                record( "db.index.explicit.auto.searchNodes",
                        "db.index.explicit.auto.searchNodes(query :: ANY?) :: (node :: NODE?, weight :: FLOAT?)",
                        "Search nodes in explicit automatic index. Replaces `START n=node:node_auto_index('key:foo*')`", "READ" ),
                record( "db.index.explicit.auto.seekNodes",
                        "db.index.explicit.auto.seekNodes(key :: STRING?, value :: ANY?) :: (node :: NODE?)",
                        "Get node from explicit automatic index. Replaces `START n=node:node_auto_index(key = 'A')`", "READ" ),
                record( "db.index.explicit.auto.searchRelationships",
                        "db.index.explicit.auto.searchRelationships(query :: ANY?) :: (relationship :: RELATIONSHIP?, weight :: FLOAT?)",
                        "Search relationship in explicit automatic index. Replaces `START r=relationship:relationship_auto_index('key:foo*')`", "READ" ),
                record( "db.index.explicit.auto.seekRelationships",
                        "db.index.explicit.auto.seekRelationships(key :: STRING?, value :: ANY?) :: " +
                        "(relationship :: RELATIONSHIP?)",
                        "Get relationship from explicit automatic index. Replaces `START r=relationship:relationship_auto_index(key = 'A')`", "READ" ),
                record( "db.index.explicit.addNode",
                        "db.index.explicit.addNode(indexName :: STRING?, node :: NODE?, key :: STRING?, value :: ANY?) :: (success :: BOOLEAN?)",
                        "Add a node to an explicit index based on a specified key and value", "WRITE" ),
                record( "db.index.explicit.addRelationship",
                        "db.index.explicit.addRelationship(indexName :: STRING?, relationship :: RELATIONSHIP?, key :: STRING?, value :: ANY?) :: " +
                        "(success :: BOOLEAN?)",
                        "Add a relationship to an explicit index based on a specified key and value", "WRITE" ),
                record( "db.index.explicit.removeNode",
                        "db.index.explicit.removeNode(indexName :: STRING?, node :: NODE?, " +
                        "key =  <[9895b15e-8693-4a21-a58b-4b7b87e09b8e]>  :: STRING?) :: (success :: BOOLEAN?)",
                        "Remove a node from an explicit index with an optional key", "WRITE" ),
                record( "db.index.explicit.removeRelationship",
                        "db.index.explicit.removeRelationship(indexName :: STRING?, relationship :: RELATIONSHIP?, " +
                        "key =  <[9895b15e-8693-4a21-a58b-4b7b87e09b8e]>  :: STRING?) :: " +
                        "(success :: BOOLEAN?)",
                        "Remove a relationship from an explicit index with an optional key", "WRITE" ),
                record( "db.index.explicit.drop",
                        "db.index.explicit.drop(indexName :: STRING?) :: " +
                        "(type :: STRING?, name :: STRING?, config :: MAP?)",
                        "Remove an explicit index - YIELD type,name,config", "WRITE" ),
                record( "db.index.explicit.forNodes",
                        "db.index.explicit.forNodes(indexName :: STRING?, config = {} :: MAP?) :: " +
                        "(type :: STRING?, name :: STRING?, config :: MAP?)",
                        "Get or create a node explicit index - YIELD type,name,config", "WRITE" ),
                record( "db.index.explicit.forRelationships",
                        "db.index.explicit.forRelationships(indexName :: STRING?, config = {} :: MAP?) :: " +
                        "(type :: STRING?, name :: STRING?, config :: MAP?)",
                        "Get or create a relationship explicit index - YIELD type,name,config", "WRITE" ),
                record( "db.index.explicit.existsForNodes",
                        "db.index.explicit.existsForNodes(indexName :: STRING?) :: (success :: BOOLEAN?)",
                        "Check if a node explicit index exists", "READ" ),
                record( "db.index.explicit.existsForRelationships",
                        "db.index.explicit.existsForRelationships(indexName :: STRING?) :: (success :: BOOLEAN?)",
                        "Check if a relationship explicit index exists", "DEFAULT" ),
                record( "db.index.explicit.list",
                        "db.index.explicit.list() :: (type :: STRING?, name :: STRING?, config :: MAP?)",
                        "List all explicit indexes - YIELD type,name,config", "READ" ),
                record( "dbms.components",
                        "dbms.components() :: (name :: STRING?, versions :: LIST? OF STRING?, edition :: STRING?)",
                        "List DBMS components and their versions.", "READ" ),
                record( "dbms.procedures",
                        "dbms.procedures() :: (name :: STRING?, signature :: STRING?, description :: STRING?, mode :: STRING?)",
                        "List all procedures in the DBMS.", "DBMS" ),
                record( "dbms.functions",
                        "dbms.functions() :: (name :: STRING?, signature :: STRING?, description :: STRING?)",
                        "List all user functions in the DBMS.", "DBMS" ),
                record( "dbms.queryJmx",
                        "dbms.queryJmx(query :: STRING?) :: (name :: STRING?, description :: STRING?, attributes :: " +
                        "MAP?)",
                        "Query JMX management data by domain and name. For instance, \"org.neo4j:*\"", "READ" ),
                record( "dbms.clearQueryCaches",
                        "dbms.clearQueryCaches() :: (value :: STRING?)",
                        "Clears all query caches.", "DBMS" ),
                record( "db.createIndex",
                        "db.createIndex(index :: STRING?, providerName :: STRING?) :: (index :: STRING?, providerName :: STRING?, status :: STRING?)",
                        "Create a schema index with specified index provider (for example: CALL db.createIndex(\":Person(name)\", \"lucene+native-2.0\")) - " +
                                "YIELD index, providerName, status", "SCHEMA" ),
                record( "db.createUniquePropertyConstraint",
                        "db.createUniquePropertyConstraint(index :: STRING?, providerName :: STRING?) :: " +
                                "(index :: STRING?, providerName :: STRING?, status :: STRING?)",
                        "Create a unique property constraint with index backed by specified index provider " +
                                "(for example: CALL db.createUniquePropertyConstraint(\":Person(name)\", \"lucene+native-2.0\")) - " +
                                "YIELD index, providerName, status", "SCHEMA" )
        ) );
    }

    @Test
    public void shouldListSystemComponents() throws Throwable
    {
        // When/Then
        assertThat( call( "dbms.components" ), contains(
                record( "Neo4j Kernel", singletonList( "1.3.37" ), "enterprise" )
        ) );
    }

    @Test
    public void shouldCloseStatementIfExceptionIsThrownDbLabels()
    {
        // Given
        RuntimeException runtimeException = new RuntimeException();
        when( tokens.labelsGetAllTokens() ).thenThrow( runtimeException );

        // When
        try
        {
            call( "db.labels" );
            fail( "Procedure call should have failed" );
        }
        catch ( ProcedureException e )
        {
            assertThat( e.getCause(), is( runtimeException ) );
            // expected
        }

        // Then
        verify( statement ).close();
    }

    @Test
    public void shouldCloseStatementIfExceptionIsThrownDbPropertyKeys()
    {
        // Given
        RuntimeException runtimeException = new RuntimeException();
        when( tokens.propertyKeyGetAllTokens() ).thenThrow( runtimeException );

        // When
        try
        {
            call( "db.propertyKeys" );
            fail( "Procedure call should have failed" );
        }
        catch ( ProcedureException e )
        {
            assertThat( e.getCause(), is( runtimeException ) );
            // expected
        }

        // Then
        verify( statement ).close();
    }

    @Test
    public void shouldCloseStatementIfExceptionIsThrownDbRelationshipTypes()
    {
        // Given
        RuntimeException runtimeException = new RuntimeException();
        when( tokens.relationshipTypesGetAllTokens() ).thenThrow( runtimeException );

        // When
        try
        {
            call( "db.relationshipTypes" );
            fail( "Procedure call should have failed" );
        }
        catch ( ProcedureException e )
        {
            assertThat( e.getCause(), is( runtimeException ) );
            // expected
        }

        // Then
        verify( statement ).close();
    }

    private Matcher<Object[]> record( Object... fields )
    {
        return equalTo( fields );
    }

    private void givenIndex( String label, String propKey )
    {
        int labelId = token( label, labels );
        int propId = token( propKey, propKeys );

        IndexReference index = DefaultIndexReference.general( labelId, propId );
        indexes.add( index );
    }

    private void givenUniqueConstraint( String label, String propKey )
    {
        int labelId = token( label, labels );
        int propId = token( propKey, propKeys );

        IndexReference index = DefaultIndexReference.unique( labelId, propId );
        uniqueIndexes.add( index );
        constraints.add( ConstraintDescriptorFactory.uniqueForLabel( labelId, propId ) );
    }

    private void givenNodePropExistenceConstraint( String label, String propKey )
    {
        int labelId = token( label, labels );
        int propId = token( propKey, propKeys );

        constraints.add( ConstraintDescriptorFactory.existsForLabel( labelId, propId ) );
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

    private Integer token( String name, Map<Integer,String> tokens )
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

    @Before
    public void setup() throws Exception
    {
        procs.registerComponent( KernelTransaction.class, ctx -> ctx.get( KERNEL_TRANSACTION ), false );
        procs.registerComponent( DependencyResolver.class, ctx -> ctx.get( DEPENDENCY_RESOLVER ), false );
        procs.registerComponent( GraphDatabaseAPI.class, ctx -> ctx.get( GRAPHDATABASEAPI ), false );
        procs.registerComponent( SecurityContext.class, ctx -> ctx.get( SECURITY_CONTEXT ), true );

        procs.registerComponent( Log.class, ctx -> ctx.get( LOG), false );
        procs.registerType( Node.class, NTNode );
        procs.registerType( Relationship.class, NTRelationship );
        procs.registerType( Path.class, NTPath );

        new SpecialBuiltInProcedures( "1.3.37", Edition.enterprise.toString() ).accept( procs );
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
        when( schemaRead.index( anyInt(), anyInt() )).thenAnswer(
                (Answer<CapableIndexReference>) invocationOnMock -> {
                    int label = invocationOnMock.getArgument( 0 );
                    int prop = invocationOnMock.getArgument( 1 );
                    for ( IndexReference index : indexes )
                    {
                        if ( index.label() == label && prop == index.properties()[0] )
                        {
                            return new DefaultCapableIndexReference( index.isUnique(), null,
                                    InMemoryIndexProviderFactory.PROVIDER_DESCRIPTOR, label, prop );
                        }
                    }
                    for ( IndexReference index : uniqueIndexes )
                    {
                        if ( index.label() == label && prop == index.properties()[0] )
                        {
                            return new DefaultCapableIndexReference( index.isUnique(), null,
                                    InMemoryIndexProviderFactory.PROVIDER_DESCRIPTOR, label, prop );
                        }
                    }
                    throw new AssertionError(  );
                } );
        when( schemaRead.constraintsGetAll() ).thenAnswer( i -> constraints.iterator() );

        when( tokens.propertyKeyName( anyInt() ) )
                .thenAnswer( invocation -> propKeys.get( invocation.getArgument( 0 ) ) );
        when( tokens.nodeLabelName( anyInt() ) ).thenAnswer( invocation -> labels.get( invocation.getArgument( 0 ) ) );
        when( tokens.relationshipTypeName( anyInt() ) )
                .thenAnswer( invocation -> relTypes.get( invocation.getArgument( 0 ) ) );

        // Make it appear that labels are in use
        // TODO: We really should just have `labelsInUse()` on the Kernel API directly,
        //       it'd make testing much easier.
        when( schemaRead.constraintsGetForRelationshipType( anyInt() ) ).thenReturn( emptyIterator() );
        when( schemaRead.indexesGetForLabel( anyInt() ) ).thenReturn( emptyIterator() );
        when( schemaRead.constraintsGetForLabel( anyInt() ) ).thenReturn( emptyIterator() );
        when( read.countsForNode( anyInt() ) ).thenReturn( 1L );
        when( read.countsForRelationship( anyInt(), anyInt(), anyInt() ) ).thenReturn( 1L );
        when( schemaRead.indexGetState( any( IndexReference.class ) ) ).thenReturn( InternalIndexState.ONLINE );
    }

    private Answer<Iterator<NamedToken>> asTokens( Map<Integer,String> tokens )
    {
        return i -> tokens.entrySet().stream()
                              .map( entry -> new NamedToken( entry.getValue(), entry.getKey() ) )
                              .iterator();
    }

    private List<Object[]> call( String name, Object... args ) throws ProcedureException
    {
        BasicContext ctx = new BasicContext();
        ctx.put( KERNEL_TRANSACTION, tx );
        ctx.put( DEPENDENCY_RESOLVER, resolver );
        ctx.put( GRAPHDATABASEAPI, graphDatabaseAPI );
        ctx.put( SECURITY_CONTEXT, SecurityContext.AUTH_DISABLED );
        ctx.put( LOG, log );
        when( graphDatabaseAPI.getDependencyResolver() ).thenReturn( resolver );
        when( resolver.resolveDependency( Procedures.class ) ).thenReturn( procs );
        return Iterators.asList( procs.callProcedure(
                ctx, ProcedureSignature.procedureName( name.split( "\\." ) ), args, resourceTracker ) );
    }

    private static final Key<DependencyResolver> DEPENDENCY_RESOLVER =
            Key.key( "DependencyResolver", DependencyResolver.class );

    private static final Key<GraphDatabaseAPI> GRAPHDATABASEAPI =
            Key.key( "GraphDatabaseAPI", GraphDatabaseAPI.class );

    private static final Key<Log> LOG =
            Key.key( "Log", Log.class );
}

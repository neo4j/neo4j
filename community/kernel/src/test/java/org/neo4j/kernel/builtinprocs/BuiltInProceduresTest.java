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
import java.util.function.Supplier;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.proc.BasicContext;
import org.neo4j.kernel.api.proc.Key;
import org.neo4j.kernel.api.proc.ProcedureSignature;
import org.neo4j.kernel.api.schema.constaints.ConstraintDescriptor;
import org.neo4j.kernel.api.schema.constaints.ConstraintDescriptorFactory;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.api.schema.index.IndexDescriptorFactory;
import org.neo4j.kernel.api.security.SecurityContext;
import org.neo4j.kernel.impl.factory.Edition;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.impl.proc.TypeMappers;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.Token;

import static java.util.Collections.emptyIterator;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.api.proc.Context.KERNEL_TRANSACTION;
import static org.neo4j.kernel.api.proc.Context.SECURITY_CONTEXT;
import static org.neo4j.kernel.api.proc.Neo4jTypes.NTNode;
import static org.neo4j.kernel.api.proc.Neo4jTypes.NTPath;
import static org.neo4j.kernel.api.proc.Neo4jTypes.NTRelationship;

public class BuiltInProceduresTest
{
    private final List<IndexDescriptor> indexes = new LinkedList<>();
    private final List<IndexDescriptor> uniqueIndexes = new LinkedList<>();
    private final List<ConstraintDescriptor> constraints = new LinkedList<>();
    private final Map<Integer,String> labels = new HashMap<>();
    private final Map<Integer,String> propKeys = new HashMap<>();
    private final Map<Integer,String> relTypes = new HashMap<>();

    private final ReadOperations read = mock( ReadOperations.class );
    private final Statement statement = mock( Statement.class );
    private final KernelTransaction tx = mock( KernelTransaction.class );
    private final DependencyResolver resolver = mock( DependencyResolver.class );
    private final GraphDatabaseAPI graphDatabaseAPI = mock( GraphDatabaseAPI.class );

    private final Procedures procs = new Procedures();

    @Test
    public void shouldListAllIndexes() throws Throwable
    {
        // Given
        givenIndex( "User", "name" );

        // When/Then
        assertThat( call( "db.indexes" ),
                contains( record( "INDEX ON :User(name)", "ONLINE", "node_label_property" ) ) );
    }

    @Test
    public void shouldListAllUniqueIndexes() throws Throwable
    {
        // Given
        givenUniqueConstraint( "User", "name" );

        // When/Then
        assertThat( call( "db.indexes" ),
                contains( record( "INDEX ON :User(name)", "ONLINE", "node_unique_property" ) ) );
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
                        "List the currently active config of Neo4j." ),
                record( "db.awaitIndex", "db.awaitIndex(index :: STRING?, timeOutSeconds = 300 :: INTEGER?) :: VOID",
                        "Wait for an index to come online (for example: CALL db.awaitIndex(\":Person(name)\"))." ),
                record( "db.awaitIndexes", "db.awaitIndexes(timeOutSeconds = 300 :: INTEGER?) :: VOID",
                        "Wait for all indexes to come online (for example: CALL db.awaitIndexes(\"500\"))." ),
                record( "db.constraints", "db.constraints() :: (description :: STRING?)",
                        "List all constraints in the database." ),
                record( "db.indexes", "db.indexes() :: (description :: STRING?, state :: STRING?, type :: STRING?)",
                        "List all indexes in the database." ),
                record( "db.labels", "db.labels() :: (label :: STRING?)", "List all labels in the database." ),
                record( "db.propertyKeys", "db.propertyKeys() :: (propertyKey :: STRING?)",
                        "List all property keys in the database." ),
                record( "db.relationshipTypes", "db.relationshipTypes() :: (relationshipType :: STRING?)",
                        "List all relationship types in the database." ),
                record( "db.resampleIndex", "db.resampleIndex(index :: STRING?) :: VOID",
                        "Schedule resampling of an index (for example: CALL db.resampleIndex(\":Person(name)\"))." ),
                record( "db.resampleOutdatedIndexes", "db.resampleOutdatedIndexes() :: VOID",
                        "Schedule resampling of all outdated indexes." ),
                record( "db.schema",
                        "db.schema() :: (nodes :: LIST? OF NODE?, relationships :: LIST? OF RELATIONSHIP?)",
                        "Show the schema of the data." ),
                record( "db.index.explicit.searchNodes",
                        "db.index.explicit.searchNodes(indexName :: STRING?, query :: ANY?) :: (node :: NODE?, weight :: FLOAT?)",
                        "Search nodes from explicit index. Replaces `START n=node:nodes('key:foo*')`"),
                record( "db.index.explicit.seekNodes",
                        "db.index.explicit.seekNodes(indexName :: STRING?, key :: STRING?, value :: ANY?) :: (node :: NODE?)",
                        "Get node from explicit index. Replaces `START n=node:nodes(key = 'A')`"),
                record( "db.index.explicit.searchRelationships",
                        "db.index.explicit.searchRelationships(indexName :: STRING?, query :: ANY?) :: " +
                                "(relationship :: RELATIONSHIP?, weight :: FLOAT?)",
                        "Search relationship from explicit index. Replaces `START r=relationship:relIndex('key:foo*')`"),
                record( "db.index.explicit.searchRelationshipsIn",
                        "db.index.explicit.searchRelationshipsIn(indexName :: STRING?, in :: NODE?, query :: ANY?) :: " +
                                "(relationship :: RELATIONSHIP?, weight :: FLOAT?)",
                        "Search relationship from explicit index, starting at the node 'in'."),
                record( "db.index.explicit.searchRelationshipsOut",
                        "db.index.explicit.searchRelationshipsOut(indexName :: STRING?, out :: NODE?, query :: ANY?) :: " +
                                "(relationship :: RELATIONSHIP?, weight :: FLOAT?)",
                        "Search relationship from explicit index, ending at the node 'out'."),
                record( "db.index.explicit.searchRelationshipsBetween",
                        "db.index.explicit.searchRelationshipsBetween(indexName :: STRING?, in :: NODE?, out :: NODE?, query :: ANY?) :: " +
                                "(relationship :: RELATIONSHIP?, weight :: FLOAT?)",
                        "Search relationship from explicit index, starting at the node 'in' and ending at 'out'."),
                record( "db.index.explicit.seekRelationships",
                        "db.index.explicit.seekRelationships(indexName :: STRING?, key :: STRING?, value :: ANY?) :: " +
                        "(relationship :: RELATIONSHIP?)",
                        "Get relationship from explicit index. Replaces `START r=relationship:relIndex(key = 'A')`"),
                record( "db.index.explicit.auto.searchNodes",
                        "db.index.explicit.auto.searchNodes(query :: ANY?) :: (node :: NODE?, weight :: FLOAT?)",
                        "Search nodes from explicit automatic index. Replaces `START n=node:node_auto_index('key:foo*')`"),
                record( "db.index.explicit.auto.seekNodes",
                        "db.index.explicit.auto.seekNodes(key :: STRING?, value :: ANY?) :: (node :: NODE?)",
                        "Get node from explicit automatic index. Replaces `START n=node:node_auto_index(key = 'A')`"),
                record( "db.index.explicit.auto.searchRelationships",
                        "db.index.explicit.auto.searchRelationships(query :: ANY?) :: (relationship :: RELATIONSHIP?, weight :: FLOAT?)",
                        "Search relationship from explicit automatic index. Replaces `START r=relationship:relationship_auto_index('key:foo*')`"),
                record( "db.index.explicit.auto.seekRelationships",
                        "db.index.explicit.auto.seekRelationships(key :: STRING?, value :: ANY?) :: " +
                        "(relationship :: RELATIONSHIP?)",
                        "Get relationship from explicit automatic index. Replaces `START r=relationship:relationship_auto_index(key = 'A')`"),
                record( "db.index.explicit.addNode",
                        "db.index.explicit.addNode(indexName :: STRING?, node :: NODE?, key :: STRING?, value :: ANY?) :: (success :: BOOLEAN?)",
                        "Add a node to a explicit index based on a specified key and value"),
                record( "db.index.explicit.addRelationship",
                        "db.index.explicit.addRelationship(indexName :: STRING?, relationship :: RELATIONSHIP?, key :: STRING?, value :: ANY?) :: " +
                        "(success :: BOOLEAN?)",
                        "Add a relationship to a explicit index based on a specified key and value"),
                record( "db.index.explicit.removeNode",
                        "db.index.explicit.removeNode(indexName :: STRING?, node :: NODE?, key :: STRING?) :: (success :: BOOLEAN?)",
                        "Remove a node from a explicit index with an optional key"),
                record( "db.index.explicit.removeRelationship",
                        "db.index.explicit.removeRelationship(indexName :: STRING?, relationship :: RELATIONSHIP?, key :: STRING?) :: " +
                        "(success :: BOOLEAN?)",
                        "Remove a relationship from a explicit index with an optional key"),
                record( "db.index.explicit.drop",
                        "db.index.explicit.drop(indexName :: STRING?) :: " +
                        "(type :: STRING?, name :: STRING?, config :: MAP?)",
                        "Remove a explicit index - YIELD type,name,config"),
                record( "db.index.explicit.forNodes",
                        "db.index.explicit.forNodes(indexName :: STRING?) :: " +
                        "(type :: STRING?, name :: STRING?, config :: MAP?)",
                        "Get or create a node explicit index - YIELD type,name,config"),
                record( "db.index.explicit.forRelationships",
                        "db.index.explicit.forRelationships(indexName :: STRING?) :: " +
                        "(type :: STRING?, name :: STRING?, config :: MAP?)",
                        "Get or create a relationship explicit index - YIELD type,name,config"),
                record( "db.index.explicit.existsForNodes",
                        "db.index.explicit.existsForNodes(indexName :: STRING?) :: (success :: BOOLEAN?)",
                        "Check if a node explicit index exists"),
                record( "db.index.explicit.existsForRelationships",
                        "db.index.explicit.existsForRelationships(indexName :: STRING?) :: (success :: BOOLEAN?)",
                        "Check if a relationship explicit index exists"),
                record( "db.index.explicit.list",
                        "db.index.explicit.list() :: (type :: STRING?, name :: STRING?, config :: MAP?)",
                        "List all explicit indexes - YIELD type,name,config"),
                record( "dbms.components",
                        "dbms.components() :: (name :: STRING?, versions :: LIST? OF STRING?, edition :: STRING?)",
                        "List DBMS components and their versions." ),
                record( "dbms.procedures",
                        "dbms.procedures() :: (name :: STRING?, signature :: STRING?, description :: STRING?)",
                        "List all procedures in the DBMS." ),
                record( "dbms.functions",
                        "dbms.functions() :: (name :: STRING?, signature :: STRING?, description :: STRING?)",
                        "List all user functions in the DBMS." ),
                record( "dbms.queryJmx",
                        "dbms.queryJmx(query :: STRING?) :: (name :: STRING?, description :: STRING?, attributes :: " +
                        "MAP?)",
                        "Query JMX management data by domain and name. For instance, \"org.neo4j:*\"" )
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
    public void shouldCloseStatementIfExceptionIsThrownDbLabels() throws Throwable
    {
        // Given
        RuntimeException runtimeException = new RuntimeException();
        when( read.labelsGetAllTokens() ).thenThrow( runtimeException );

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
    public void shouldCloseStatementIfExceptionIsThrownDbPropertyKeys() throws Throwable
    {
        // Given
        RuntimeException runtimeException = new RuntimeException();
        when( read.propertyKeyGetAllTokens() ).thenThrow( runtimeException );

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
    public void shouldCloseStatementIfExceptionIsThrownDRelationshipTypes() throws Throwable
    {
        // Given
        RuntimeException runtimeException = new RuntimeException();
        when( read.relationshipTypesGetAllTokens() ).thenThrow( runtimeException );

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

        IndexDescriptor index = IndexDescriptorFactory.forLabel( labelId, propId );
        indexes.add( index );
    }

    private void givenUniqueConstraint( String label, String propKey )
    {
        int labelId = token( label, labels );
        int propId = token( propKey, propKeys );

        IndexDescriptor index = IndexDescriptorFactory.uniqueForLabel( labelId, propId );
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
        Supplier<Integer> allocateFromMap = () ->
        {
            int newIndex = tokens.size();
            tokens.put( newIndex, name );
            return newIndex;
        };
        return tokens.entrySet().stream()
                     .filter( entry -> entry.getValue().equals( name ) )
                     .map( Map.Entry::getKey )
                     .findFirst().orElseGet( allocateFromMap );
    }

    @Before
    public void setup() throws Exception
    {
        procs.registerComponent( KernelTransaction.class, ctx -> ctx.get( KERNEL_TRANSACTION ), false );
        procs.registerComponent( DependencyResolver.class, ctx -> ctx.get( DEPENDENCY_RESOLVER ), false );
        procs.registerComponent( GraphDatabaseAPI.class, ctx -> ctx.get( GRAPHDATABASEAPI ), false );
        procs.registerComponent( SecurityContext.class, ctx -> ctx.get( SECURITY_CONTEXT ), true );

        procs.registerType( Node.class, new TypeMappers.SimpleConverter( NTNode, Node.class ) );
        procs.registerType( Relationship.class, new TypeMappers.SimpleConverter( NTRelationship, Relationship.class ) );
        procs.registerType( Path.class, new TypeMappers.SimpleConverter( NTPath, Path.class ) );

        new SpecialBuiltInProcedures( "1.3.37", Edition.enterprise.toString() ).accept( procs );
        procs.registerProcedure( BuiltInProcedures.class );
        procs.registerProcedure( BuiltInDbmsProcedures.class );

        when( tx.acquireStatement() ).thenReturn( statement );
        when( statement.readOperations() ).thenReturn( read );

        when( read.propertyKeyGetAllTokens() ).thenAnswer( asTokens( propKeys ) );
        when( read.labelsGetAllTokens() ).thenAnswer( asTokens( labels ) );
        when( read.relationshipTypesGetAllTokens() ).thenAnswer( asTokens( relTypes ) );
        when( read.indexesGetAll() ).thenAnswer(
                i -> Iterators.concat( indexes.iterator(), uniqueIndexes.iterator() ) );
        when( read.constraintsGetAll() ).thenAnswer( i -> constraints.iterator() );
        when( read.proceduresGetAll() ).thenReturn( procs.getAllProcedures() );

        when( read.propertyKeyGetName( anyInt() ) )
                .thenAnswer( invocation -> propKeys.get( invocation.getArguments()[0] ) );
        when( read.labelGetName( anyInt() ) )
                .thenAnswer( invocation -> labels.get( invocation.getArguments()[0] ) );
        when( read.relationshipTypeGetName( anyInt() ) )
                .thenAnswer( invocation -> relTypes.get( invocation.getArguments()[0] ) );

        // Make it appear that labels are in use
        // TODO: We really should just have `labelsInUse()` on the Kernel API directly,
        //       it'd make testing much easier.
        when( read.constraintsGetForRelationshipType( anyInt() ) ).thenReturn( emptyIterator() );
        when( read.indexesGetForLabel( anyInt() ) ).thenReturn( emptyIterator() );
        when( read.constraintsGetForLabel( anyInt() ) ).thenReturn( emptyIterator() );
        when( read.countsForNode( anyInt() ) ).thenReturn( 1L );
        when( read.countsForRelationship( anyInt(), anyInt(), anyInt() ) ).thenReturn( 1L );
        when( read.indexGetState( any( IndexDescriptor.class ) ) ).thenReturn( InternalIndexState.ONLINE );
    }

    private Answer<Iterator<Token>> asTokens( Map<Integer,String> tokens )
    {
        return i -> tokens.entrySet().stream()
                              .map( entry -> new Token( entry.getValue(), entry.getKey() ) )
                              .iterator();
    }

    private List<Object[]> call( String name, Object... args ) throws ProcedureException
    {
        BasicContext ctx = new BasicContext();
        ctx.put( KERNEL_TRANSACTION, tx );
        ctx.put( DEPENDENCY_RESOLVER, resolver );
        ctx.put( GRAPHDATABASEAPI, graphDatabaseAPI );
        ctx.put( SECURITY_CONTEXT, SecurityContext.AUTH_DISABLED );
        when( graphDatabaseAPI.getDependencyResolver() ).thenReturn( resolver );
        when( resolver.resolveDependency( Procedures.class ) ).thenReturn( procs );
        return Iterators
                .asList( procs.callProcedure( ctx, ProcedureSignature.procedureName( name.split( "\\." ) ), args ) );
    }

    private static final Key<DependencyResolver> DEPENDENCY_RESOLVER =
            Key.key( "DependencyResolver", DependencyResolver.class );

    private static final Key<GraphDatabaseAPI> GRAPHDATABASEAPI =
            Key.key( "GraphDatabaseAPI", GraphDatabaseAPI.class );
}

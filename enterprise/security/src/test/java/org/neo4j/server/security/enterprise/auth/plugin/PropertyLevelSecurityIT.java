/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.server.security.enterprise.auth.plugin;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.enterprise.api.security.EnterpriseAuthManager;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Procedure;
import org.neo4j.server.security.enterprise.auth.EnterpriseAuthAndUserManager;
import org.neo4j.server.security.enterprise.auth.EnterpriseUserManager;
import org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles;
import org.neo4j.server.security.enterprise.configuration.SecuritySettings;
import org.neo4j.test.TestEnterpriseGraphDatabaseFactory;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.neo4j.internal.kernel.api.Transaction.Type.explicit;
import static org.neo4j.server.security.auth.SecurityTestUtils.authToken;

public class PropertyLevelSecurityIT
{
    @Rule
    public TestDirectory testDirectory = TestDirectory.testDirectory();

    private GraphDatabaseFacade db;
    private LoginContext neo;
    private LoginContext smith;
    private LoginContext morpheus;
    private LoginContext jones;

    @Before
    public void setUp() throws Throwable
    {
        TestGraphDatabaseFactory s = new TestEnterpriseGraphDatabaseFactory();
        db = (GraphDatabaseFacade) s.newImpermanentDatabaseBuilder( testDirectory.graphDbDir() )
                .setConfig( SecuritySettings.property_level_authorization_enabled, "true" )
                .setConfig( SecuritySettings.property_level_authorization_permissions, "Agent=alias,secret" )
                .setConfig( SecuritySettings.procedure_roles, "test.*:procRole" )
                .setConfig( GraphDatabaseSettings.auth_enabled, "true" )
                .newGraphDatabase();
        EnterpriseAuthAndUserManager authManager = (EnterpriseAuthAndUserManager) db.getDependencyResolver().resolveDependency( EnterpriseAuthManager.class );
        Procedures procedures = db.getDependencyResolver().resolveDependency( Procedures.class );
        procedures.registerProcedure( TestProcedure.class );
        EnterpriseUserManager userManager = authManager.getUserManager();
        userManager.newUser( "Neo", "eon", false );
        userManager.newUser( "Smith", "mr", false );
        userManager.newUser( "Jones", "mr", false );
        userManager.newUser( "Morpheus", "dealwithit", false );

        userManager.newRole( "procRole", "Jones" );
        userManager.newRole( "Agent", "Smith", "Jones" );

        userManager.addRoleToUser( PredefinedRoles.ARCHITECT, "Neo" );
        userManager.addRoleToUser( PredefinedRoles.READER, "Smith" );
        userManager.addRoleToUser( PredefinedRoles.READER, "Morpheus" );

        neo = authManager.login( authToken( "Neo", "eon" ) );
        smith = authManager.login( authToken( "Smith", "mr" ) );
        jones = authManager.login( authToken( "Jones", "mr" ) );
        morpheus = authManager.login( authToken( "Morpheus", "dealwithit" ) );
    }

    @After
    public void tearDown()
    {
        db.shutdown();
    }

    @Test
    public void shouldNotShowRestrictedTokensForRestrictedUser() throws Throwable
    {
        Result result = execute( neo, "CREATE (n {name: 'Andersson', alias: 'neo'}) ", Collections.emptyMap() );
        assertThat( result.getQueryStatistics().getNodesCreated(), equalTo( 1 ) );
        assertThat( result.getQueryStatistics().getPropertiesSet(), equalTo( 2 ) );
        result.close();
        execute( smith, "MATCH (n) WHERE n.name = 'Andersson' RETURN n, n.alias as alias", Collections.emptyMap(), r ->
        {
            assertThat( r.hasNext(), equalTo( true ) );
            assertThat( r.next().get( "alias" ), equalTo( null ) );
        } );
    }

    @Test
    public void shouldShowRestrictedTokensForUnrestrictedUser() throws Throwable
    {
        Result result = execute( neo, "CREATE (n {name: 'Andersson', alias: 'neo'}) ", Collections.emptyMap() );
        assertThat( result.getQueryStatistics().getNodesCreated(), equalTo( 1 ) );
        assertThat( result.getQueryStatistics().getPropertiesSet(), equalTo( 2 ) );
        result.close();
        execute( morpheus, "MATCH (n) WHERE n.name = 'Andersson' RETURN n, n.alias as alias", Collections.emptyMap(), r ->
        {
            assertThat( r.hasNext(), equalTo( true ) );
            assertThat( r.next().get( "alias" ), equalTo( "neo" ) );
        } );
    }

    @Test
    public void shouldBehaveLikeDataIsMissing() throws Throwable
    {
        execute( neo, "CREATE (n {name: 'Andersson'}) ", Collections.emptyMap() ).close();

        String query = "MATCH (n) WHERE n.name = 'Andersson' RETURN n.alias as alias";

        execute( neo, query, Collections.emptyMap(), r ->
        {
            assertThat( r.hasNext(), equalTo( true ) );
            assertThat( r.next().get( "alias" ), equalTo( null ) );
        } );

        execute( neo, "MATCH (n {name: 'Andersson'}) SET n.alias = 'neo' ", Collections.emptyMap() ).close();

        execute( smith, query, Collections.emptyMap(), r ->
        {
            assertThat( r.hasNext(), equalTo( true ) );
            assertThat( r.next().get( "alias" ), equalTo( null ) );
        } );
    }

    @Test
    public void shouldBehaveLikeDataIsMissingWhenFiltering() throws Throwable
    {
        execute( neo, "CREATE (n {name: 'Andersson'}) ", Collections.emptyMap() ).close();

        String query = "MATCH (n) WHERE n.alias = 'neo' RETURN n";

        execute( neo, query, Collections.emptyMap(), r -> assertThat( r.hasNext(), equalTo( false ) ) );

        execute( neo, "MATCH (n {name: 'Andersson'}) SET n.alias = 'neo' ", Collections.emptyMap() ).close();

        execute( smith, query, Collections.emptyMap(), r -> assertThat( r.hasNext(), equalTo( false ) ) );
        execute( neo, query, Collections.emptyMap(), r -> assertThat( r.hasNext(), equalTo( true ) ) );
    }

    @Test
    public void shouldBehaveLikeDataIsMissingForKeys() throws Throwable
    {
        execute( neo, "CREATE (n {name: 'Andersson'}) ", Collections.emptyMap() ).close();

        String query = "MATCH (n) RETURN keys(n) AS keys";

        execute( neo, query, Collections.emptyMap(), r ->
        {
            assertThat( r.hasNext(), equalTo( true ) );
            assertThat( r.next().get( "keys" ), equalTo( Collections.singletonList( "name" ) ) );
        } );

        execute( neo, "MATCH (n {name: 'Andersson'}) SET n.alias = 'neo' ", Collections.emptyMap() ).close();

        execute( smith, query, Collections.emptyMap(), r ->
        {
            assertThat( r.hasNext(), equalTo( true ) );
            assertThat( r.next().get( "keys" ), equalTo( Collections.singletonList( "name" ) ) );
        } );

        execute( neo, query, Collections.emptyMap(), r ->
        {
            assertThat( r.hasNext(), equalTo( true ) );
            assertThat( (Iterable<String>) r.next().get( "keys" ), contains( "name", "alias" ) );
        } );
    }

    @Test
    public void shouldBehaveLikeDataIsMissingForProperties() throws Throwable
    {
        execute( neo, "CREATE (n {name: 'Andersson'}) ", Collections.emptyMap() ).close();

        String query = "MATCH (n) RETURN properties(n) AS props";

        execute( neo, query, Collections.emptyMap(), r ->
        {
            assertThat( r.hasNext(), equalTo( true ) );
            assertThat( r.next().get( "props" ), equalTo( Collections.singletonMap( "name", "Andersson" ) ) );
        } );

        execute( neo, "MATCH (n {name: 'Andersson'}) SET n.alias = 'neo' ", Collections.emptyMap() ).close();

        execute( smith, query, Collections.emptyMap(), r ->
        {
            assertThat( r.hasNext(), equalTo( true ) );
            assertThat( r.next().get( "props" ), equalTo( Collections.singletonMap( "name", "Andersson" ) ) );
        } );

        Map<String, String> expected = new HashMap<>(  );
        expected.put( "name", "Andersson" );
        expected.put( "alias", "neo" );
        execute( neo, query, Collections.emptyMap(), r ->
        {
            assertThat( r.hasNext(), equalTo( true ) );
            assertThat( r.next().get( "props" ), equalTo( expected ) );
        } );
    }

    @Test
    public void shouldBehaveLikeDataIsMissingForExists() throws Throwable
    {
        execute( neo, "CREATE (n:Person {name: 'Andersson'}) ", Collections.emptyMap() ).close();

        String query = "MATCH (n:Person) WHERE exists(n.alias) RETURN n.alias";

        execute( neo, query, Collections.emptyMap(), r -> assertThat( r.hasNext(), equalTo( false ) ) );

        execute( neo, "MATCH (n {name: 'Andersson'}) SET n.alias = 'neo' ", Collections.emptyMap() ).close();

        execute( neo, query, Collections.emptyMap(), r ->
        {
            assertThat( r.hasNext(), equalTo( true ) );
            assertThat( r.next().get( "n.alias" ), equalTo( "neo" ) );
        } );

        execute( smith, query, Collections.emptyMap(), r -> assertThat( r.hasNext(), equalTo( false ) ) );
    }

    @Test
    public void shouldBehaveLikeDataIsMissingForStringBegins() throws Throwable
    {
        execute( neo, "CREATE (n:Person {name: 'Andersson'}) ", Collections.emptyMap() ).close();

        String query = "MATCH (n:Person) WHERE n.alias starts with 'n' RETURN n.alias";

        execute( neo, query, Collections.emptyMap(), r -> assertThat( r.hasNext(), equalTo( false ) ) );

        execute( neo, "MATCH (n {name: 'Andersson'}) SET n.alias = 'neo' ", Collections.emptyMap() ).close();

        execute( neo, query, Collections.emptyMap(), r ->
        {
            assertThat( r.hasNext(), equalTo( true ) );
            assertThat( r.next().get( "n.alias" ), equalTo( "neo" ) );
        } );

        execute( smith, query, Collections.emptyMap(), r -> assertThat( r.hasNext(), equalTo( false ) ) );
    }

    @Test
    public void shouldBehaveLikeDataIsMissingForNotContains() throws Throwable
    {
        execute( neo, "CREATE (n:Person {name: 'Andersson'}) ", Collections.emptyMap() ).close();

        String query = "MATCH (n:Person) WHERE NOT n.alias contains 'eo' RETURN n.alias, n.name";

        execute( neo, query, Collections.emptyMap(), r -> assertThat( r.hasNext(), equalTo( false ) ) );

        execute( neo, "MATCH (n {name: 'Andersson'}) SET n.alias = 'neo' ", Collections.emptyMap() ).close();
        execute( neo, "CREATE (n:Person {name: 'Betasson', alias: 'beta'}) ", Collections.emptyMap() ).close();
        execute( neo, "CREATE (n:Person {name: 'Cetasson'}) ", Collections.emptyMap() ).close();

        execute( neo, query, Collections.emptyMap(), r ->
        {
            assertThat( r.hasNext(), equalTo( true ) );
            Map<String,Object> next = r.next();
            assertThat( next.get( "n.alias" ), equalTo( "beta" ) );
            assertThat( next.get( "n.name" ), equalTo( "Betasson" ) );
            assertThat( r.hasNext(), equalTo( false ) );
        } );

        execute( smith, query, Collections.emptyMap(), r -> assertThat( r.hasNext(), equalTo( false ) ) );
    }

    @Test
    public void shouldBehaveLikeDataIsMissingForRange() throws Throwable
    {
        execute( neo, "CREATE (n:Person {name: 'Andersson'}) ", Collections.emptyMap() ).close();

        String query = "MATCH (n:Person) WHERE n.secret > 10 RETURN n.secret";

        execute( neo, query, Collections.emptyMap(), r -> assertThat( r.hasNext(), equalTo( false ) ) );

        execute( neo, "MATCH (n {name: 'Andersson'}) SET n.secret = 42 ", Collections.emptyMap() ).close();

        execute( neo, query, Collections.emptyMap(), r ->
        {
            assertThat( r.hasNext(), equalTo( true ) );
            assertThat( r.next().get( "n.secret" ), equalTo( 42L ) );
        } );

        execute( smith, query, Collections.emptyMap(), r -> assertThat( r.hasNext(), equalTo( false ) ) );
    }

    @Test
    public void shouldBehaveLikeDataIsMissingForCompositeQuery() throws Throwable
    {
        execute( neo, "CREATE (n:Person {name: 'Andersson'}) ", Collections.emptyMap() ).close();

        String query = "MATCH (n:Person) WHERE n.name = 'Andersson' and n.alias = 'neo' RETURN n.alias";

        execute( neo, query, Collections.emptyMap(), r -> assertThat( r.hasNext(), equalTo( false ) ) );

        execute( neo, "MATCH (n {name: 'Andersson'}) SET n.alias = 'neo' ", Collections.emptyMap() ).close();

        execute( neo, query, Collections.emptyMap(), r ->
        {
            assertThat( r.hasNext(), equalTo( true ) );
            assertThat( r.next().get( "n.alias" ), equalTo( "neo" ) );
        } );

        execute( smith, query, Collections.emptyMap(), r -> assertThat( r.hasNext(), equalTo( false ) ) );
    }

    // INDEX

    @Test
    public void shouldBehaveLikeDataIsMissingWhenFilteringWithIndex() throws Throwable
    {
        execute( neo, "CREATE (n:Person {name: 'Andersson'})", Collections.emptyMap() ).close();
        execute( neo, "CREATE INDEX ON :Person(alias)", Collections.emptyMap() ).close();
        execute( neo, "CALL db.awaitIndexes", Collections.emptyMap() ).close();

        String query = "MATCH (n:Person) USING INDEX n:Person(alias) WHERE n.alias = 'neo' RETURN n";

        execute( neo, query, Collections.emptyMap(), r -> assertThat( r.hasNext(), equalTo( false ) ) );

        execute( neo, "MATCH (n {name: 'Andersson'}) SET n.alias = 'neo' ", Collections.emptyMap() ).close();

        execute( smith, query, Collections.emptyMap(), r ->
        {
            assertThat( r.getExecutionPlanDescription().toString(), containsString( "NodeIndexSeek" ) );
            assertThat( r.hasNext(), equalTo( false ) );
        } );

        execute( neo, query, Collections.emptyMap(), r -> assertThat( r.hasNext(), equalTo( true ) ) );
    }

    @Test
    public void shouldBehaveLikeDataIsMissingForExistsWithIndex() throws Throwable
    {
        execute( neo, "CREATE INDEX ON :Person(alias)", Collections.emptyMap() ).close();
        execute( neo, "CALL db.awaitIndexes", Collections.emptyMap() ).close();
        execute( neo, "CREATE (n:Person {name: 'Andersson'}) ", Collections.emptyMap() ).close();

        String query = "MATCH (n:Person) USING INDEX n:Person(alias) WHERE exists(n.alias) RETURN n.alias";

        execute( neo, query, Collections.emptyMap(), r -> assertThat( r.hasNext(), equalTo( false ) ) );

        execute( neo, "MATCH (n {name: 'Andersson'}) SET n.alias = 'neo' ", Collections.emptyMap() ).close();

        execute( neo, query, Collections.emptyMap(), r ->
        {
            assertThat( r.getExecutionPlanDescription().toString(), containsString( "NodeIndexScan" ) );
            assertThat( r.hasNext(), equalTo( true ) );
            assertThat( r.next().get( "n.alias" ), equalTo( "neo" ) );
        } );

        execute( smith, query, Collections.emptyMap(), r -> assertThat( r.hasNext(), equalTo( false ) ) );
    }

    @Test
    public void shouldBehaveLikeDataIsMissingForStringBeginsWithIndex() throws Throwable
    {
        execute( neo, "CREATE INDEX ON :Person(alias)", Collections.emptyMap() ).close();
        execute( neo, "CALL db.awaitIndexes", Collections.emptyMap() ).close();
        execute( neo, "CREATE (n:Person {name: 'Andersson'}) ", Collections.emptyMap() ).close();

        String query = "MATCH (n:Person) USING INDEX n:Person(alias) WHERE n.alias starts with 'n' RETURN n.alias";

        execute( neo, query, Collections.emptyMap(), r -> assertThat( r.hasNext(), equalTo( false ) ) );

        execute( neo, "MATCH (n {name: 'Andersson'}) SET n.alias = 'neo' ", Collections.emptyMap() ).close();

        execute( neo, query, Collections.emptyMap(), r ->
        {
            assertThat( r.getExecutionPlanDescription().toString(), containsString( "NodeIndexSeekByRange" ) );
            assertThat( r.hasNext(), equalTo( true ) );
            assertThat( r.next().get( "n.alias" ), equalTo( "neo" ) );
        } );

        execute( smith, query, Collections.emptyMap(), r -> assertThat( r.hasNext(), equalTo( false ) ) );
    }

    @Test
    public void shouldBehaveLikeDataIsMissingForRangeWithIndex() throws Throwable
    {
        execute( neo, "CREATE INDEX ON :Person(secret)", Collections.emptyMap() ).close();
        execute( neo, "CALL db.awaitIndexes", Collections.emptyMap() ).close();
        execute( neo, "CREATE (n:Person {name: 'Andersson'}) ", Collections.emptyMap() ).close();

        String query = "MATCH (n:Person) USING INDEX n:Person(secret) WHERE n.secret > 10 RETURN n.secret";

        execute( neo, query, Collections.emptyMap(), r -> assertThat( r.hasNext(), equalTo( false ) ) );

        execute( neo, "MATCH (n {name: 'Andersson'}) SET n.secret = 42 ", Collections.emptyMap() ).close();

        execute( neo, query, Collections.emptyMap(), r ->
        {
            assertThat( r.getExecutionPlanDescription().toString(), containsString( "NodeIndexSeek" ) );
            assertThat( r.hasNext(), equalTo( true ) );
            assertThat( r.next().get( "n.secret" ), equalTo( 42L ) );
        } );

        execute( smith, query, Collections.emptyMap(), r -> assertThat( r.hasNext(), equalTo( false ) ) );
    }

    @Test
    public void shouldBehaveLikeDataIsMissingForCompositeWithIndex() throws Throwable
    {
        execute( neo, "CREATE INDEX ON :Person(name , alias)", Collections.emptyMap() ).close();
        execute( neo, "CREATE INDEX ON :Person(name)", Collections.emptyMap() ).close();
        execute( neo, "CALL db.awaitIndexes", Collections.emptyMap() ).close();
        execute( neo, "CREATE (n:Person {name: 'Andersson'}) ", Collections.emptyMap() ).close();

        String query = "MATCH (n:Person) USING INDEX n:Person(name, alias) WHERE n.name = 'Andersson' and n.alias = 'neo' RETURN n.alias";

        execute( neo, query, Collections.emptyMap(), r -> assertThat( r.hasNext(), equalTo( false ) ) );

        execute( neo, "MATCH (n {name: 'Andersson'}) SET n.alias = 'neo' ", Collections.emptyMap() ).close();

        execute( neo, query, Collections.emptyMap(), r ->
        {
            assertThat( r.getExecutionPlanDescription().toString(), containsString( "NodeIndexSeek" ) );
            assertThat( r.hasNext(), equalTo( true ) );
            assertThat( r.next().get( "n.alias" ), equalTo( "neo" ) );
        } );

        execute( smith, query, Collections.emptyMap(), r -> assertThat( r.hasNext(), equalTo( false ) ) );
    }

    // RELATIONSHIPS

    @Test
    public void shouldBehaveLikeDataIsMissingForRelationshipProperties() throws Throwable
    {
        execute( neo, "CREATE (n {name: 'Andersson'}) CREATE (m { name: 'Betasson'}) CREATE (n)-[:Neighbour]->(m)", Collections.emptyMap() ).close();

        String query = "MATCH (n)-[r]->(m) WHERE n.name = 'Andersson' AND m.name = 'Betasson' RETURN properties(r) AS props";

        execute( neo, query, Collections.emptyMap(), r ->
        {
            assertThat( r.hasNext(), equalTo( true ) );
            assertThat( r.next().get( "props" ), equalTo( Collections.emptyMap() ) );
        } );

        execute( neo, "MATCH (n {name: 'Andersson'})-[r]->({name: 'Betasson'}) SET r.secret = 'lovers' ", Collections.emptyMap() ).close();

        execute( smith, query, Collections.emptyMap(), r ->
        {
            assertThat( r.hasNext(), equalTo( true ) );
            assertThat( r.next().get( "props" ), equalTo( Collections.emptyMap() ) );
        } );

        execute( neo, query, Collections.emptyMap(), r ->
        {
            assertThat( r.hasNext(), equalTo( true ) );
            assertThat( r.next().get( "props" ), equalTo( Collections.singletonMap( "secret", "lovers" ) ) );
        } );
    }

    // PROCS

    @Test
    public void shouldBehaveWithProcedures() throws Throwable
    {
        execute( neo, "CREATE (n:Person {name: 'Andersson'}) ", Collections.emptyMap() ).close();

        String query = "CALL db.propertyKeys() YIELD propertyKey RETURN propertyKey ORDER BY propertyKey";

        execute( neo, query, Collections.emptyMap(), r ->
        {
            assertThat( r.hasNext(), equalTo( true ) );
            assertThat( r.next().get( "propertyKey" ), equalTo( "name" ) );
            assertThat( r.hasNext(), equalTo( false ) );
        } );

        execute( neo, "MATCH (n {name: 'Andersson'}) SET n.alias = 'neo' ", Collections.emptyMap() ).close();

        execute( smith, query, Collections.emptyMap(), r ->
        {
            assertThat( r.hasNext(), equalTo( true ) );
            assertThat( r.next().get( "propertyKey" ), equalTo( "name" ) );
            assertThat( r.hasNext(), equalTo( false ) );
        } );

        execute( neo, query, Collections.emptyMap(), r ->
        {
            assertThat( r.hasNext(), equalTo( true ) );
            assertThat( r.next().get( "propertyKey" ), equalTo( "alias" ) );
            assertThat( r.hasNext(), equalTo( true ) );
            assertThat( r.next().get( "propertyKey" ), equalTo( "name" ) );
            assertThat( r.hasNext(), equalTo( true ) );
            assertThat( r.next().get( "propertyKey" ), equalTo( "secret" ) );
            assertThat( r.hasNext(), equalTo( false ) );
        } );
    }

    @Test
    public void allowedProcedureShouldIgnorePropertyBlacklist() throws Throwable
    {
        execute( neo, "CREATE (:Person {name: 'Andersson'}) ", Collections.emptyMap() ).close();

        assertProcedureResult( morpheus, Collections.singletonMap( "Andersson", "N/A" ) );
        assertProcedureResult( smith, Collections.singletonMap( "Andersson", "N/A" ) );
        assertProcedureResult( jones, Collections.singletonMap( "Andersson", "N/A" ) );

        execute( neo, "MATCH (n:Person) WHERE n.name = 'Andersson' SET n.alias = 'neo' RETURN n", Collections.emptyMap() ).close();

        assertProcedureResult( morpheus, Collections.singletonMap( "Andersson", "neo" ) );
        assertProcedureResult( smith, Collections.singletonMap( "Andersson", "N/A" ) );
        assertProcedureResult( jones, Collections.singletonMap( "Andersson", "neo" ) );
    }

    private void assertProcedureResult( LoginContext user, Map<String,String> nameAliasMap )
    {
        execute( user, "CALL test.getAlias", Collections.emptyMap(), r ->
        {
            assertThat( r.hasNext(), equalTo( true ) );
            Map<String,Object> next = r.next();
            String name = (String) next.get( "name" );
            assertThat( nameAliasMap.containsKey( name ), equalTo( true ) );
            assertThat( next.get( "alias" ), equalTo( nameAliasMap.get( name ) ) );
        } );
    }

    private void execute( LoginContext subject, String query, Map<String,Object> params, Consumer<Result> consumer )
    {
        Result result;
        try ( InternalTransaction tx = db.beginTransaction( explicit, subject ) )
        {
            result = db.execute( tx, query, ValueUtils.asMapValue( params ) );
            consumer.accept( result );
            tx.success();
            result.close();
        }
    }

    private Result execute( LoginContext subject, String query, Map<String,Object> params )
    {
        Result result;
        try ( InternalTransaction tx = db.beginTransaction( explicit, subject ) )
        {
            result = db.execute( tx, query, ValueUtils.asMapValue( params ) );
            tx.success();
        }
        return result;
    }

    @SuppressWarnings( "unused" )
    public static class TestProcedure
    {
        @Context
        public GraphDatabaseService db;

        @Procedure( name = "test.getAlias", mode = Mode.READ )
        public Stream<MyOutputRecord> getAlias()
        {
            ResourceIterator<Node> nodes = db.findNodes( Label.label( "Person" ) );
            return nodes
                    .stream()
                    .map( n -> new MyOutputRecord( (String) n.getProperty( "name" ),
                                                   (String) n.getProperty( "alias", "N/A" ) ) );
        }
    }

    @SuppressWarnings( "WeakerAccess" )
    public static class MyOutputRecord
    {
        public String name;
        public String alias;

        public MyOutputRecord( String name, String alias )
        {
            this.name = name;
            this.alias = alias;
        }
    }
}

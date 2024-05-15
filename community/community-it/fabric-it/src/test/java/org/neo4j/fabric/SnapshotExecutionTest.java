/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.fabric;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import reactor.core.publisher.Flux;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.TransientException;
import org.neo4j.driver.types.Path;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.BoltDbmsExtension;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.internal.helpers.Strings.joinAsLines;
import static org.neo4j.kernel.api.exceptions.Status.Statement.ArithmeticError;
import static org.neo4j.kernel.api.exceptions.Status.Statement.SyntaxError;

@BoltDbmsExtension( configurationCallback = "configure" )
@TestInstance( TestInstance.Lifecycle.PER_CLASS )
class SnapshotExecutionTest
{
    @Inject
    private static GraphDatabaseAPI graphDatabase;

    @Inject
    private static ConnectorPortRegister connectorPortRegister;

    @Inject
    private static GlobalProcedures procedureRegistry;

    private static Driver driver;

    @ExtensionCallback
    static void configure( TestDatabaseManagementServiceBuilder builder )
    {
        builder.setConfig( GraphDatabaseInternalSettings.snapshot_query, true );
    }

    @BeforeAll
    static void beforeAll() throws KernelException
    {
        driver = DriverUtils.createDriver( connectorPortRegister );
        procedureRegistry.registerProcedure( ConcurrentQuery.class );
    }

    @AfterAll
    static void afterAll()
    {
        driver.close();
    }

    @BeforeEach
    void beforeEach()
    {
        try ( Transaction tx = driver.session().beginTransaction() )
        {
            tx.run( "MATCH (n) DETACH DELETE n" );
            tx.run( "CREATE (:First {number: 0})" ).consume();
            tx.run( "CREATE (:Second {number: 0})" ).consume();
            tx.commit();
        }
    }

    @Test
    void testBasicSnapshotRead()
    {
        var query = joinAsLines(
                "MATCH (f:First)",
                "WITH f.number AS f",
                "CALL se.doConcurrently('MATCH (f:First)\n SET f.number=1')",
                "CALL se.doConcurrently('MATCH (s:Second)\n SET s.number=1')",
                "MATCH (s:Second)",
                "RETURN f, s.number AS s"
        );

        var result = driver.session().run( query ).stream()
                           .map( r -> List.of( r.get( "f", -1 ), r.get( "s", -1 ) ) )
                           .findFirst()
                           .get();
        // The result would be (0, 1) without Snapshot EE
        assertEquals( List.of( 1, 1 ), result );
    }

    /**
     * Entity being deleted is a special isolation conflict, because Cypher execution throws exception.
     * This test checks that Snapshot EE retries in this case instead of failing the query.
     */
    @Test
    void testRetryWhenEntityDeleted()
    {
        var query = joinAsLines(
                "MATCH (f:First)",
                "WITH f AS f",
                "CALL se.doConcurrently('MATCH (f:First)\nDELETE f')",
                "RETURN f"
        );

        var result = driver.session().writeTransaction( t -> t.run( query ).list() );
        assertEquals( List.of(), result );
    }

    @Test
    void testDoNotRetryWrites()
    {
        var query = joinAsLines(
                "MATCH (f:First)",
                "SET f.number=3",
                "WITH f.number AS f",
                "CALL se.doConcurrently('MATCH (s:Second)\n SET s.number=1')",
                "MATCH (s:Second)",
                "RETURN f, s.number AS s"
        );

        var e = assertThrows( TransientException.class, () -> driver.session().run( query ).list() );
        assertThat( e )
                .hasMessageContaining( "Unable to get clean data snapshot for query 'MATCH (`f`:`First`)" )
                .hasMessageContaining( "RETURN `f` AS `f`, (`s`).`number` AS `s`' that performs updates." );
    }

    // As hard as it might be to believe, we actually had a bug in this area.
    // Since most tests work with primitives (storable values), this is a smoke test that
    // collection virtual types work as result of Snapshot EE
    @Test
    void testCollectionVirtualTypes()
    {
        var query = "RETURN { key: 'Hello', listKey: [{ inner: 'Map1' }, { inner: 'Map2' }]} AS m";

        Value m = driver.session().run( query ).stream().map( r -> r.get( "m" ) ).findFirst().get();

        var expected = Values.value( Map.of(
                "key", "Hello",
                "listKey", List.of(
                        Map.of( "inner", "Map1" ),
                        Map.of( "inner", "Map2" )
                )
        ) );

        assertEquals( expected, m );
    }

    // Since most test work with primitives (storable values), this is a smoke test that
    // entity types work as result of Snapshot EE
    @Test
    void testEntityVirtualTypes()
    {
        var query = joinAsLines(
                "CREATE (n1:LABEL_1), (n2:LABEL_2)",
                "MERGE (n1) - [:TYPE_1] -> (n2)",
                "RETURN () - [] -> ()[0] AS p"
        );

        Path p = driver.session().run( query ).stream().map( r -> r.get( "p" ) ).findFirst().get().asPath();
        assertThat( p.start().labels() ).containsExactly( "LABEL_1" );
        assertThat( p.end().labels() ).containsExactly( "LABEL_2" );
    }

    // Cypher query execution has two phases, this tests an error during the first one.
    @Test
    void testErrorInExecutionPhaseOfTheQuery()
    {
        var query = "UNWIND[1, 0] AS a RETURN b";
        var e = assertThrows( ClientException.class, () -> driver.session().run( query ).list() );
        assertEquals( SyntaxError.code().serialize(), e.code() );
        assertThat( e ).hasMessageContaining( "Variable `b` not defined" );
    }

    // Cypher query execution has two phases, this tests an error during the second one.
    @Test
    void testErrorInResultStreamingPhaseOfTheQuery()
    {
        var query = joinAsLines(
                "UNWIND [3, 2, 1, 0] AS a",
                "RETURN 1/a AS a"
        );

        var e = assertThrows( ClientException.class, () -> driver.session().run( query ).list() );
        assertEquals( ArithmeticError.code().serialize(), e.code() );
        assertEquals( "/ by zero", e.getMessage() );
    }

    // Even though Snapshot EE materialises the result
    // it should not violate semantic of a streaming protocol,
    // so for instance if the client asks for x records, it should get x.
    @Test
    void testResultStreaming()
    {
        var query = joinAsLines(
                "UNWIND range(0, 100) AS a",
                "RETURN a"
        );

        int receivedRecords = Flux.from( driver.rxSession().run( query ).records() )
                                  .limitRate( 5 )
                                  .take( 50 )
                                  .collectList()
                                  .block()
                                  .size();
        assertEquals( 50, receivedRecords );
    }

    public static class ConcurrentQuery
    {

        @Context
        public GraphDatabaseService db;

        @Procedure( mode = Mode.WRITE, name = "se.doConcurrently" )
        public void doConcurrently( @Name( "query" ) String query ) throws Exception
        {
            var executor = Executors.newSingleThreadExecutor();
            try
            {
                var future = executor.submit( () -> db.executeTransactionally( query ) );
                future.get();
            }
            finally
            {
                executor.shutdown();
            }
        }
    }
}

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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.reactive.RxResult;
import org.neo4j.driver.reactive.RxTransaction;
import org.neo4j.test.extension.BoltDbmsExtension;
import org.neo4j.test.extension.Inject;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static org.assertj.core.api.Assertions.assertThat;

@BoltDbmsExtension
@TestInstance( TestInstance.Lifecycle.PER_CLASS )
class BoltLocalResultStreamTest
{

    @Inject
    private static ConnectorPortRegister connectorPortRegister;

    private static Driver driver;

    @BeforeAll
    static void beforeAll()
    {
        driver = DriverUtils.createDriver( connectorPortRegister );
    }

    @AfterAll
    static void tearDown()
    {
        driver.close();
    }

    @Test
    void testBasicResultStream()
    {
        List<String> result = inTx( tx ->
                tx.run( "UNWIND range(0, 4) AS i RETURN 'r' + i as A" ).stream()
                        .map( r -> r.get( "A" ).asString() )
                        .collect( Collectors.toList() )
        );

        assertThat( result ).isEqualTo( List.of( "r0", "r1", "r2", "r3", "r4" ) );
    }

    @Test
    void testRxResultStream()
    {
        List<String> result = inRxTx( tx ->
        {
            RxResult statementResult = tx.run( "UNWIND range(0, 4) AS i RETURN 'r' + i as A" );
            return Flux.from( statementResult.records() )
                    .limitRate( 1 )
                    .collectList()
                    .block()
                    .stream()
                    .map( r -> r.get( "A" ).asString() )
                    .collect( Collectors.toList() );
        } );

        assertThat( result ).isEqualTo( List.of( "r0", "r1", "r2", "r3", "r4" ) );
    }

    @Test
    void testPartialStream()
    {
        List<String> result  = inRxTx( tx ->
        {
            RxResult statementResult = tx.run( "UNWIND range(0, 4) AS i RETURN 'r' + i as A" );

            return Flux.from( statementResult.records() )
                    .limitRequest( 2 )
                    .collectList()
                    .block()
                    .stream()
                    .map( r -> r.get( "A" ).asString() )
                    .collect( Collectors.toList() );
        } );

        assertThat( result ).isEqualTo( List.of( "r0", "r1" ) );
    }

    private static <T> T inTx( Function<Transaction,T> workload )
    {
        try ( var session = driver.session() )
        {
            return session.writeTransaction( workload::apply );
        }
    }

    private static <T> T inRxTx( Function<RxTransaction,T> workload )
    {
        var session = driver.rxSession();
        try
        {
            RxTransaction tx = Mono.from( session.beginTransaction() ).block();
            try
            {
                return workload.apply( tx );
            }
            finally
            {
                Mono.from( tx.rollback() ).block();
            }
        }
        finally
        {
            Mono.from( session.close() ).block();
        }
    }
}

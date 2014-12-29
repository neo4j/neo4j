/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.server.rest.transactional.integration;

import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.server.rest.AbstractRestFunctionalTestBase;
import org.neo4j.test.server.HTTP;
import org.neo4j.tooling.GlobalGraphOperations;

import static java.util.UUID.randomUUID;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertTrue;
import static org.neo4j.test.server.HTTP.RawPayload.rawPayload;

public class ConcurrentMergesIT extends AbstractRestFunctionalTestBase
{
    @Test
    public void concurrentMergesWithUniqueConstraintShouldNotProduceDuplicates() throws InterruptedException
    {
        // Given: label, property key, sequence of property values with duplicates, query with MERGE clause, constraint
        Label personLabel = DynamicLabel.label( "Person" );
        String uuidProperty = "uuid";
        UuidSequence uuids = new UuidSequence();

        String cypherQuery = "MERGE (p:" + personLabel + " {" + uuidProperty + ": {uuid}})<-[:HAS]-(d:Dog) " +
                             "ON CREATE SET d.breed = 'Beagle', p.created = timestamp()";


        createConstraint( personLabel, uuidProperty );

        HTTP.Builder http = HTTP.withBaseUri( getBaseUri() );
        ExecutorService executor = Executors.newFixedThreadPool( Runtime.getRuntime().availableProcessors() * 2 );

        // When: lots of threads executing query concurrently, query is parameterized with duplicated property values
        for ( int i = 0; i < 3_000; i++ )
        {
            executor.submit( executeStatement( http, cypherQuery, "uuid", uuids ) );
        }
        executor.shutdown();
        executor.awaitTermination( 30, SECONDS );

        // Then: no duplicated property values should exist, constraint should be respected
        Map<String,Set<Long>> duplicatesByValue = findDuplicates( personLabel, uuidProperty );
        assertTrue( "\nDuplicates found:\n" + duplicatesByValue, duplicatesByValue.isEmpty() );
    }

    private void createConstraint( Label label, String propertyKey )
    {
        try ( Transaction tx = graphdb().beginTx() )
        {
            graphdb().schema().constraintFor( label ).assertPropertyIsUnique( propertyKey ).create();
            tx.success();
        }

        try ( Transaction tx = graphdb().beginTx() )
        {
            graphdb().schema().awaitIndexesOnline( 10, SECONDS );
            tx.success();
        }
    }

    private Runnable executeStatement( final HTTP.Builder http, final String cypherQuery, final String paramName,
            final UuidSequence uuids )
    {
        return new Runnable()
        {
            @Override
            public void run()
            {
                http.withHeaders(
                        "Accept", "application/json; charset=UTF-8",
                        "Content-Type", "application/json"
                ).POST( getDataUri() + "transaction/commit",
                        statements( cypherQuery, paramName, uuids.next() ) );
            }
        };
    }

    private HTTP.RawPayload statements( String cypherQuery, String paramName, String paramValue )
    {
        return rawPayload( "{" +
                           "  \"statements\" : [ {" +
                           "    \"statement\" : \"" + cypherQuery + "\"," +
                           "    \"parameters\" : {" +
                           "      \"" + paramName + "\" : \"" + paramValue + "\"" +
                           "    }" +
                           "  } ]" +
                           "}" );
    }

    private Map<String,Set<Long>> findDuplicates( Label label, String propertyKey )
    {
        Map<String,Set<Long>> propertyValue2NodeIds = new HashMap<>();

        try ( Transaction tx = graphdb().beginTx() )
        {
            for ( Node node : GlobalGraphOperations.at( graphdb() ).getAllNodesWithLabel( label ) )
            {
                if ( node.hasProperty( propertyKey ) )
                {
                    String propertyValue = String.valueOf( node.getProperty( propertyKey ) );
                    Set<Long> nodeIds = propertyValue2NodeIds.get( propertyValue );
                    if ( nodeIds == null )
                    {
                        propertyValue2NodeIds.put( propertyValue, nodeIds = new HashSet<>() );
                    }
                    nodeIds.add( node.getId() );
                }
            }
            tx.success();
        }

        Map<String,Set<Long>> duplicates = new HashMap<>();

        for ( Map.Entry<String,Set<Long>> entry : propertyValue2NodeIds.entrySet() )
        {
            if ( entry.getValue().size() > 1 )
            {
                duplicates.put( entry.getKey(), entry.getValue() );
            }
        }

        return duplicates;
    }

    private static class UuidSequence
    {
        volatile UUID uuid = randomUUID();

        String next()
        {
            UUID currentUuid = uuid;
            if ( ThreadLocalRandom.current().nextBoolean() )
            {
                uuid = randomUUID();
            }
            return currentUuid.toString();
        }
    }
}

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
package org.neo4j.kernel;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.io.ByteUnit;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

import static org.apache.commons.lang3.RandomStringUtils.randomAscii;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.reserved_page_header_bytes;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.internal.helpers.collection.Iterators.count;

@TestDirectoryExtension
class PageBytesReserveIT
{
    @Inject
    private TestDirectory testDirectory;

    public static IntStream pageSizes()
    {
        return IntStream.of( 8, 16, 24, 32, 40, 48, 64, 1024, 2048, (int) (ByteUnit.kibiBytes( 8 ) - 256) );
    }

    @ParameterizedTest
    @MethodSource( "pageSizes" )
    void reserveBytesInPageHeader( int reservedBytes )
    {
        var managementService =
                new TestDatabaseManagementServiceBuilder( testDirectory.homePath() ).setConfig( reserved_page_header_bytes, reservedBytes ).build();
        try
        {
            Label testLabel = Label.label( "test" );
            RelationshipType testRelType = RelationshipType.withName( "test" );
            String repPropertyKey = "a";
            String nodePropertyKey = "b";
            String relProperty = randomAscii( (int) ByteUnit.kibiBytes( 8 ) );
            String nodeProperty = randomAscii( (int) ByteUnit.kibiBytes( 8 ) );
            GraphDatabaseService database = managementService.database( DEFAULT_DATABASE_NAME );
            int iterations = 1024;
            for ( int i = 0; i < iterations; i++ )
            {
                try ( Transaction transaction = database.beginTx() )
                {
                    Node start = transaction.createNode( testLabel );
                    Node end = transaction.createNode( testLabel );
                    Relationship relationshipTo = start.createRelationshipTo( end, testRelType );
                    relationshipTo.setProperty( repPropertyKey, relProperty );
                    start.setProperty( nodePropertyKey, nodeProperty );
                    transaction.commit();
                }
            }

            try ( Transaction transaction = database.beginTx() )
            {
                assertEquals( 2 * iterations, count( transaction.findNodes( testLabel ) ) );
            }
        }
        finally
        {
            managementService.shutdown();
        }
    }

    @ParameterizedTest
    @MethodSource( "pageSizes" )
    void reserveBytesInPageHeaderWithAdditionalIndexes( int reservedBytes )
    {
        var managementService =
                new TestDatabaseManagementServiceBuilder( testDirectory.homePath() ).setConfig( reserved_page_header_bytes, reservedBytes ).build();
        try
        {
            Label testLabel = Label.label( "test" );
            RelationshipType testRelType = RelationshipType.withName( "test" );
            String repPropertyKey = "a";
            String nodePropertyKey = "b";
            String relProperty = randomAscii( (int) ByteUnit.kibiBytes( 7 ) );
            String nodeProperty = randomAscii( (int) ByteUnit.kibiBytes( 7 ) );
            GraphDatabaseService database = managementService.database( DEFAULT_DATABASE_NAME );

            try ( Transaction tx = database.beginTx() )
            {
                Schema schema = tx.schema();
                schema.indexFor( testLabel ).on( nodePropertyKey ).withName( "nodeIndex" ).create();
                schema.indexFor( testRelType ).on( relProperty ).withName( "relIndex" ).create();
                tx.commit();
            }

            awaitIndexes( database );

            int iterations = 512;
            for ( int i = 0; i < iterations; i++ )
            {
                try ( Transaction transaction = database.beginTx() )
                {
                    Node start = transaction.createNode( testLabel );
                    Node end = transaction.createNode( testLabel );
                    Relationship relationshipTo = start.createRelationshipTo( end, testRelType );
                    relationshipTo.setProperty( repPropertyKey, relProperty );
                    start.setProperty( nodePropertyKey, nodeProperty );
                    transaction.commit();
                }
            }

            try ( Transaction transaction = database.beginTx() )
            {
                assertEquals( 2 * iterations, count( transaction.findNodes( testLabel ) ) );
            }
        }
        finally
        {
            managementService.shutdown();
        }
    }

    private static void awaitIndexes( GraphDatabaseService database )
    {
        try ( Transaction tx = database.beginTx() )
        {
            tx.schema().awaitIndexesOnline( 10, TimeUnit.MINUTES );
        }
    }
}

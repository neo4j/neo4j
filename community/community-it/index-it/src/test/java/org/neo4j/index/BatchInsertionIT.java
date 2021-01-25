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
package org.neo4j.index;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;

import org.neo4j.batchinsert.BatchInserter;
import org.neo4j.batchinsert.BatchInserters;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.id.IdValidator;
import org.neo4j.internal.id.ReservedIdException;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.internal.helpers.collection.Iterators.count;
import static org.neo4j.internal.helpers.collection.MapUtil.map;

@Neo4jLayoutExtension
public class BatchInsertionIT
{
    @Inject
    private FileSystemAbstraction fileSystem;
    @Inject
    private DatabaseLayout databaseLayout;
    private DatabaseManagementService managementService;

    @AfterEach
    void tearDown()
    {
        if ( managementService != null )
        {
            managementService.shutdown();
        }
    }

    @Test
    void shouldIndexNodesWithMultipleLabels() throws Exception
    {
        try ( var inserter = BatchInserters.inserter( databaseLayout, fileSystem ) )
        {
            inserter.createNode( map( "name", "Bob" ), label( "User" ), label( "Admin" ) );

            inserter.createDeferredSchemaIndex( label( "User" ) ).on( "name" ).create();
            inserter.createDeferredSchemaIndex( label( "Admin" ) ).on( "name" ).create();
        }

        var db = getDatabase();
        try ( Transaction tx = db.beginTx() )
        {
            assertThat( count( tx.findNodes( label( "User" ), "name", "Bob" ) ) ).isEqualTo( 1L );
            assertThat( count( tx.findNodes( label( "Admin" ), "name", "Bob" ) ) ).isEqualTo( 1L );
        }
    }

    @Test
    void shouldNotIndexNodesWithWrongLabel() throws Exception
    {
        try ( BatchInserter inserter = BatchInserters.inserter( databaseLayout, fileSystem ) )
        {
            inserter.createNode( map( "name", "Bob" ), label( "User" ), label( "Admin" ) );
            inserter.createDeferredSchemaIndex( label( "Banana" ) ).on( "name" ).create();
        }

        var db = getDatabase();
        try ( Transaction tx = db.beginTx() )
        {
            assertThat( count( tx.findNodes( label( "Banana" ), "name", "Bob" ) ) ).isEqualTo( 0L );
        }
    }

    @Test
    void shouldBeAbleToMakeRepeatedCallsToSetNodeProperty() throws Exception
    {
        final Object finalValue = 87;
        BatchInserter inserter = BatchInserters.inserter( databaseLayout, fileSystem );
        long nodeId = inserter.createNode( Collections.emptyMap() );

        inserter.setNodeProperty( nodeId, "a", "some property value" );
        inserter.setNodeProperty( nodeId, "a", 42 );
        inserter.setNodeProperty( nodeId, "a", 3.14 );
        inserter.setNodeProperty( nodeId, "a", true );
        inserter.setNodeProperty( nodeId, "a", finalValue );
        inserter.shutdown();

        var db = getDatabase();
        try ( Transaction tx = db.beginTx() )
        {
            assertThat( tx.getNodeById( nodeId ).getProperty( "a" ) ).isEqualTo( finalValue );
        }
    }

    @Test
    void shouldBeAbleToMakeRepeatedCallsToSetNodePropertyWithMultiplePropertiesPerBlock() throws Exception
    {
        BatchInserter inserter = BatchInserters.inserter( databaseLayout, fileSystem );
        long nodeId = inserter.createNode( Collections.emptyMap() );

        final Object finalValue1 = 87;
        final Object finalValue2 = 3.14;
        inserter.setNodeProperty( nodeId, "a", "some property value" );
        inserter.setNodeProperty( nodeId, "a", 42 );
        inserter.setNodeProperty( nodeId, "b", finalValue2 );
        inserter.setNodeProperty( nodeId, "a", finalValue2 );
        inserter.setNodeProperty( nodeId, "a", true );
        inserter.setNodeProperty( nodeId, "a", finalValue1 );
        inserter.shutdown();

        var db = getDatabase();
        try ( Transaction tx = db.beginTx() )
        {
            assertThat( tx.getNodeById( nodeId ).getProperty( "a" ) ).isEqualTo( finalValue1 );
            assertThat( tx.getNodeById( nodeId ).getProperty( "b" ) ).isEqualTo( finalValue2 );
        }
    }

    @Test
    void makeSureCantCreateNodeWithMagicNumber() throws IOException
    {
        try ( BatchInserter inserter = BatchInserters.inserter( databaseLayout, fileSystem ) )
        {
            assertThrows( ReservedIdException.class, () -> inserter.createNode( IdValidator.INTEGER_MINUS_ONE, null ) );
        }
    }

    private GraphDatabaseService getDatabase()
    {
        if ( managementService == null )
        {
            managementService = new TestDatabaseManagementServiceBuilder( databaseLayout ).setFileSystem( fileSystem ).build();
        }
        return managementService.database( DEFAULT_DATABASE_NAME );
    }
}

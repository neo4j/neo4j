/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.restore;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;

import org.neo4j.dbms.DatabaseManagementSystemSettings;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class RestoreDatabaseCommandTest
{
    @Rule
    public final TestDirectory directory = TestDirectory.testDirectory();

    @Test
    public void shouldNotCopyOverAndExistingDatabase() throws Exception
    {
        // given
        FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
        String databaseName = "to";
        Config config = configWith(  Config.empty(), databaseName);

        File fromPath = new File( directory.absolutePath(), "from" );
        File toPath = config.get( DatabaseManagementSystemSettings.database_path );

        createDbAt( fromPath, 0 );
        createDbAt( toPath, 0 );

        try
        {
            // when

            new RestoreDatabaseCommand( fs, fromPath, config, databaseName, false ).execute();
            fail( "Should have thrown exception" );
        }
        catch ( IllegalArgumentException exception )
        {
            // then
            assertTrue( exception.getMessage(), exception.getMessage().contains(
                    "Database with name [to] already exists" ) );
        }
    }

    @Test
    public void shouldThrowExceptionIfBackupDirectoryDoesNotExist() throws Exception
    {
        // given
        FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
        String databaseName = "to";
        Config config = configWith(  Config.empty(), databaseName);

        File fromPath = new File( directory.absolutePath(), "from" );
        File toPath = config.get( DatabaseManagementSystemSettings.database_path );

        createDbAt( toPath, 0 );

        try
        {
            // when

            new RestoreDatabaseCommand( fs, fromPath, config, databaseName, false ).execute();
            fail( "Should have thrown exception" );
        }
        catch ( IllegalArgumentException exception )
        {
            // then
            assertTrue( exception.getMessage(),
                    exception.getMessage().contains( "Source directory does not exist" ) );
        }
    }

    @Test
    public void shouldAllowForcedCopyOverAnExistingDatabase() throws Exception
    {
        // given
        FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
        String databaseName = "to";
        Config config = configWith(  Config.empty(), databaseName);

        File fromPath = new File( directory.absolutePath(), "from" );
        File toPath = config.get( DatabaseManagementSystemSettings.database_path );
        int fromNodeCount = 10;
        int toNodeCount = 20;

        createDbAt( fromPath, fromNodeCount );
        createDbAt( toPath, toNodeCount );

        // when
        new RestoreDatabaseCommand( fs, fromPath, config, databaseName, true ).execute();

        // then
        GraphDatabaseService copiedDb = new GraphDatabaseFactory().newEmbeddedDatabase( toPath );

        try(Transaction ignored = copiedDb.beginTx())
        {
            assertEquals(fromNodeCount, Iterables.count( copiedDb.getAllNodes() ) );
        }

        copiedDb.shutdown();
    }

    public static Config configWith( Config config, String databaseName )
    {
        return config.with( stringMap( DatabaseManagementSystemSettings.active_database.name(), databaseName ) );
    }

    private void createDbAt( File fromPath, int nodesToCreate )
    {
        GraphDatabaseFactory factory = new GraphDatabaseFactory();

        GraphDatabaseService db = factory.newEmbeddedDatabase( fromPath );

        try(Transaction tx = db.beginTx())
        {
            for ( int i = 0; i < nodesToCreate; i++ )
            {
                db.createNode();
            }
            tx.success();
        }

        db.shutdown();
    }
}

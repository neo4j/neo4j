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
package org.neo4j.commandline.dbms;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.util.HashMap;

import org.neo4j.commandline.admin.IncorrectUsage;
import org.neo4j.commandline.admin.NullOutsideWorld;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Args;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.util.Validators;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class DatabaseImporterTest
{
    @Rule
    public final TestDirectory testDir = TestDirectory.testDirectory();

    @Test
    public void requiresFromArgument()
    {
        String[] arguments = {"--mode=database", "--database=bar"};

        try
        {
            new DatabaseImporter( Args.parse( arguments ), Config.defaults(), new NullOutsideWorld() );

            fail( "Should have thrown an exception." );
        }
        catch ( IncorrectUsage e )
        {
            assertThat( e.getMessage(), containsString( "from" ) );
        }
    }

    @Test
    public void failIfSourceIsNotAStore()
    {
        File from = testDir.directory( "empty" );
        String[] arguments = {"--mode=database", "--database=bar", "--from=" + from.getAbsolutePath()};

        try
        {
            new DatabaseImporter( Args.parse( arguments ), Config.defaults(), new NullOutsideWorld() );
            fail( "Should have thrown an exception." );
        }
        catch ( IncorrectUsage e )
        {
            assertThat( e.getMessage(), containsString( "does not contain a database" ) );
        }
    }

    @Test
    public void copiesDatabaseFromOldLocationToNewLocation() throws Exception
    {
        File home = testDir.directory( "home" );

        File from = provideStoreDirectory();
        File destination = new File( new File( new File( home, "data" ), "databases" ), "bar" );

        String[] arguments = {"--mode=database", "--database=bar", "--from=" + from.getAbsolutePath()};

        DatabaseImporter importer =
                new DatabaseImporter( Args.parse( arguments ), getConfigWith( home, "bar" ), new NullOutsideWorld() );
        assertThat( destination, not( isExistingDatabase() ) );
        importer.doImport();
        assertThat( destination, isExistingDatabase() );
    }

    @Test
    public void removesOldMessagesLog() throws Exception
    {
        File home = testDir.directory( "home" );

        File from = provideStoreDirectory();
        File oldMessagesLog = new File( from, "messages.log" );

        assertTrue( oldMessagesLog.createNewFile() );

        File destination = new File( new File( new File( home, "data" ), "databases" ), "bar" );

        String[] arguments = {"--mode=database", "--database=bar", "--from=" + from.getAbsolutePath()};
        DatabaseImporter importer =
                new DatabaseImporter( Args.parse( arguments ), getConfigWith( home, "bar" ), new NullOutsideWorld() );

        File messagesLog = new File( destination, "messages.log" );
        importer.doImport();
        assertFalse( messagesLog.exists() );
    }

    private Config getConfigWith( File homeDir, String databaseName )
    {
        HashMap<String,String> additionalConfig = new HashMap<>();
        additionalConfig.put( GraphDatabaseSettings.neo4j_home.name(), homeDir.toString() );
        additionalConfig.put( GraphDatabaseSettings.active_database.name(), databaseName );
        return Config.defaults( additionalConfig );
    }

    private File provideStoreDirectory()
    {
        File storeDir = testDir.graphDbDir();
        GraphDatabaseService db = null;
        try
        {
            db = new TestGraphDatabaseFactory().newEmbeddedDatabase( storeDir );
            try ( Transaction transaction = db.beginTx() )
            {
                db.createNode();
                transaction.success();
            }
        }
        finally
        {
            if ( db != null )
            {
                db.shutdown();
            }
        }

        return storeDir;
    }

    private Matcher<File> isExistingDatabase()
    {
        return new BaseMatcher<File>()
        {
            @Override
            public boolean matches( final Object item )
            {
                final File store = (File) item;
                try
                {
                    Validators.CONTAINS_EXISTING_DATABASE.validate( store );
                    return true;
                }
                catch ( Exception e )
                {
                    return false;
                }
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( "an existing database." );
            }

            @Override
            public void describeMismatch( final Object item, final Description description )
            {
                description.appendValue( item ).appendText( " is not an existing database." );
            }
        };
    }
}

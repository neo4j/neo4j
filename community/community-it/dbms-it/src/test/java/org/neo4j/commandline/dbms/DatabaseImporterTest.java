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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;

import org.neo4j.commandline.admin.IncorrectUsage;
import org.neo4j.dbms.database.DatabaseManagementService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.Args;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.impl.util.Validators;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.core.IsNot.not;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

@ExtendWith( TestDirectoryExtension.class )
class DatabaseImporterTest
{
    @Inject
    private TestDirectory testDir;
    private DatabaseManagementService managementService;

    @Test
    void requiresFromArgument()
    {
        String[] arguments = {"--mode=database", "--database=bar"};
        IncorrectUsage usageException = assertThrows( IncorrectUsage.class, () -> new DatabaseImporter( Args.parse( arguments ), testDir.databaseLayout() ) );
        assertThat( usageException.getMessage(), containsString( "from" ) );
    }

    @Test
    void failIfSourceIsNotAStore()
    {
        File from = testDir.directory( "empty" );
        String[] arguments = {"--mode=database", "--database=bar", "--from=" + from.getAbsolutePath()};

        IncorrectUsage usageException = assertThrows( IncorrectUsage.class, () -> new DatabaseImporter( Args.parse( arguments ), testDir.databaseLayout() ) );
        assertThat( usageException.getMessage(), containsString( "does not contain a database" ) );
    }

    @Test
    void copiesDatabaseFromOldLocationToNewLocation() throws Exception
    {
        File home = testDir.directory( "home" );

        File from = provideStoreDirectory();
        File destination = new File( new File( new File( home, "data" ), "databases" ), "bar" );

        String[] arguments = {"--mode=database", "--database=bar", "--from=" + from.getAbsolutePath()};

        DatabaseLayout barLayout = DatabaseLayout.of( destination );
        DatabaseImporter importer =
                new DatabaseImporter( Args.parse( arguments ), barLayout );
        assertThat( destination, not( isExistingDatabase() ) );
        importer.doImport();
        assertThat( destination, isExistingDatabase() );
    }

    private File provideStoreDirectory()
    {
        GraphDatabaseAPI db = null;
        File storeDir = testDir.databaseDir( "home" );
        File databaseDirectory;
        try
        {
            managementService = new TestDatabaseManagementServiceBuilder( storeDir ).build();
            db = (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
            databaseDirectory = db.databaseLayout().databaseDirectory();
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
                managementService.shutdown();
            }
        }

        return databaseDirectory;
    }

    private static Matcher<File> isExistingDatabase()
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

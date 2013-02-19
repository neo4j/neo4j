/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.storemigration;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;
import static org.junit.Assume.assumeTrue;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.changeVersionNumber;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.truncateFile;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.truncateToFixedLength;
import static org.neo4j.kernel.impl.util.FileUtils.copyRecursively;

import java.io.File;
import java.io.IOException;
import java.net.URL;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.helpers.UTF8;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.util.FileUtils;

public class UpgradableDatabaseTestIT
{
    @Before
    public void checkOperatingSystem() {
        assumeTrue( !System.getProperty( "os.name" ).startsWith( "Windows" ) );
    }

    @Test
    public void shouldAcceptTheStoresInTheSampleDatabaseAsBeingEligibleForUpgrade() throws IOException
    {
        URL legacyStoreResource = getClass().getResource( "legacystore/exampledb/neostore" );
        File resourceDirectory = new File( legacyStoreResource.getFile() ).getParentFile();
        File workingDirectory = new File( "target/" + UpgradableDatabaseTestIT.class.getSimpleName() );

        FileUtils.deleteRecursively( workingDirectory );
        assertTrue( workingDirectory.mkdirs() );

        copyRecursively( resourceDirectory, workingDirectory );

        assertTrue( new UpgradableDatabase().storeFilesUpgradeable( new File( workingDirectory, "neostore" ) ) );
    }

    @Test
    public void shouldRejectStoresIfOneFileHasIncorrectVersion() throws IOException
    {
        URL legacyStoreResource = getClass().getResource( "legacystore/exampledb/neostore" );
        File resourceDirectory = new File( legacyStoreResource.getFile() ).getParentFile();
        File workingDirectory = new File( "target/" + UpgradableDatabaseTestIT.class.getSimpleName() );

        FileUtils.deleteRecursively( workingDirectory );
        assertTrue( workingDirectory.mkdirs() );

        copyRecursively( resourceDirectory, workingDirectory );

        changeVersionNumber( fileSystem, new File( workingDirectory, "neostore.nodestore.db" ), "v0.9.5" );

        assertFalse( new UpgradableDatabase().storeFilesUpgradeable( new File( workingDirectory, "neostore" ) ) );
    }

    @Test
    public void shouldRejectStoresIfOneFileHasNoVersionAsIfNotShutDownCleanly() throws IOException
    {
        URL legacyStoreResource = getClass().getResource( "legacystore/exampledb/neostore" );
        File resourceDirectory = new File( legacyStoreResource.getFile() ).getParentFile();
        File workingDirectory = new File( "target/" + UpgradableDatabaseTestIT.class.getSimpleName() );

        FileUtils.deleteRecursively( workingDirectory );
        assertTrue( workingDirectory.mkdirs() );

        copyRecursively( resourceDirectory, workingDirectory );

        truncateFile( fileSystem, new File( workingDirectory, "neostore.nodestore.db" ), "StringPropertyStore v0.9.9" );

        assertFalse( new UpgradableDatabase().storeFilesUpgradeable( new File( workingDirectory, "neostore" ) ) );
    }

    @Test
    public void shouldRejectStoresIfOneFileShorterThanExpectedVersionString() throws IOException
    {
        URL legacyStoreResource = getClass().getResource( "legacystore/exampledb/neostore" );
        File resourceDirectory = new File( legacyStoreResource.getFile() ).getParentFile();
        File workingDirectory = new File( "target/" + UpgradableDatabaseTestIT.class.getSimpleName() );

        FileUtils.deleteRecursively( workingDirectory );
        assertTrue( workingDirectory.mkdirs() );

        copyRecursively( resourceDirectory, workingDirectory );

        int shortFileLength = 5 /* (RelationshipTypeStore.RECORD_SIZE) */ * 3;
        assertTrue( shortFileLength < UTF8.encode( "StringPropertyStore v0.9.9" ).length );
        truncateToFixedLength( fileSystem, new File( workingDirectory, "neostore.relationshiptypestore.db" ), shortFileLength );

        assertFalse( new UpgradableDatabase().storeFilesUpgradeable( new File( workingDirectory, "neostore" ) ) );
    }

    private final FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();

    @Test
    public void shouldCommunicateWhatCausesInabilityToUpgrade() throws IOException
    {
        URL legacyStoreResource = getClass().getResource( "legacystore/exampledb/neostore" );
        File resourceDirectory = new File( legacyStoreResource.getFile() ).getParentFile();
        File workingDirectory = new File( "target/" + UpgradableDatabaseTestIT.class.getSimpleName() );

        FileUtils.deleteRecursively( workingDirectory );
        assertTrue( workingDirectory.mkdirs() );

        copyRecursively( resourceDirectory, workingDirectory );

        changeVersionNumber( fileSystem, new File( workingDirectory, "neostore.nodestore.db" ), "v0.9.5" );

        try
        {
            new UpgradableDatabase().checkUpgradeable( new File( workingDirectory, "neostore" ) );
            fail( "should not have been able to upgrade" );
        }
        catch (StoreUpgrader.UnableToUpgradeException e)
        {
            assertThat( e.getMessage(), is("'neostore.nodestore.db' has a store version number that we cannot upgrade " +
                    "from. Expected 'NodeStore v0.9.9' but file is version 'NodeStore v0.9.5'.") );
        }
    }
}

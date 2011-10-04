/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.kernel.impl.nioneo.store.CommonAbstractStore.ALL_STORES_VERSION;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.changeVersionNumber;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.copyRecursively;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.defaultConfig;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.truncateFile;
import static org.neo4j.kernel.impl.util.FileUtils.deleteRecursively;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.junit.Test;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;

public class StoreUpgraderTest
{
    @Test
    public void shouldUpgradeAnOldFormatStore() throws IOException
    {
        File workingDirectory = new File( "target/" + StoreUpgraderTest.class.getSimpleName() );
        MigrationTestUtils.prepareSampleLegacyDatabase( workingDirectory );

        assertTrue( MigrationTestUtils.allStoreFilesHaveVersion( workingDirectory, "v0.9.9" ) );

        new StoreUpgrader( defaultConfig(), new AlwaysAllowedUpgradeConfiguration() ).attemptUpgrade( new File( workingDirectory, NeoStore.DEFAULT_NAME ).getPath() );

        assertTrue( MigrationTestUtils.allStoreFilesHaveVersion( workingDirectory, ALL_STORES_VERSION ) );
    }

    @Test
    public void shouldLeaveACopyOfOriginalStoreFilesInBackupDirectory() throws IOException
    {
        File workingDirectory = new File( "target/" + StoreUpgraderTest.class.getSimpleName() );
        MigrationTestUtils.prepareSampleLegacyDatabase( workingDirectory );

        new StoreUpgrader( defaultConfig(), new AlwaysAllowedUpgradeConfiguration() ).attemptUpgrade( new File( workingDirectory, NeoStore.DEFAULT_NAME ).getPath() );

        verifyFilesHaveSameContent( MigrationTestUtils.findOldFormatStoreDirectory(), new File( workingDirectory, "upgrade_backup" ) );
    }

    @Test
    public void shouldHaltUpgradeIfUpgradeConfigurationVetoesTheProcess() throws IOException
    {
        File workingDirectory = new File( "target/" + StoreUpgraderTest.class.getSimpleName() );
        MigrationTestUtils.prepareSampleLegacyDatabase( workingDirectory );

        UpgradeConfiguration vetoingUpgradeConfiguration = new UpgradeConfiguration()
        {
            public void checkConfigurationAllowsAutomaticUpgrade()
            {
                throw new UpgradeNotAllowedByConfigurationException( "vetoed" );
            }
        };

        try
        {
            new StoreUpgrader( defaultConfig(), vetoingUpgradeConfiguration ).attemptUpgrade( new File( workingDirectory, NeoStore.DEFAULT_NAME ).getPath() );
            fail( "Should throw exception" );
        } catch ( UpgradeNotAllowedByConfigurationException e )
        {
            // expected
        }
    }

    @Test
    public void shouldLeaveAllFilesUntouchedIfWrongVersionNumberFound() throws IOException
    {
        File workingDirectory = new File( "target/" + StoreUpgraderTest.class.getSimpleName() );
        File comparisonDirectory = new File( "target/" + StoreUpgraderTest.class.getSimpleName() + "-comparison" );
        MigrationTestUtils.prepareSampleLegacyDatabase( workingDirectory );

        changeVersionNumber( new File( workingDirectory, "neostore.nodestore.db" ), "v0.9.5" );
        deleteRecursively( comparisonDirectory );
        copyRecursively( workingDirectory, comparisonDirectory );

        try
        {
            new StoreUpgrader( defaultConfig(), new AlwaysAllowedUpgradeConfiguration() ).attemptUpgrade( new File( workingDirectory, NeoStore.DEFAULT_NAME ).getPath() );
            fail( "Should throw exception" );
        } catch ( StoreUpgrader.UnableToUpgradeException e )
        {
            // expected
        }

        verifyFilesHaveSameContent( comparisonDirectory, workingDirectory );
    }

    @Test
    public void shouldRefuseToUpgradeIfAnyOfTheStoresWeNotShutDownCleanly() throws IOException
    {
        File workingDirectory = new File( "target/" + StoreUpgraderTest.class.getSimpleName() );
        File comparisonDirectory = new File( "target/" + StoreUpgraderTest.class.getSimpleName() + "-comparison" );
        MigrationTestUtils.prepareSampleLegacyDatabase( workingDirectory );

        truncateFile( new File( workingDirectory, "neostore.propertystore.db.index.keys" ), "StringPropertyStore v0.9.9" );
        deleteRecursively( comparisonDirectory );
        copyRecursively( workingDirectory, comparisonDirectory );

        try
        {
            new StoreUpgrader( defaultConfig(), new AlwaysAllowedUpgradeConfiguration() ).attemptUpgrade( new File( workingDirectory, NeoStore.DEFAULT_NAME ).getPath() );
            fail( "Should throw exception" );
        } catch ( StoreUpgrader.UnableToUpgradeException e )
        {
            // expected
        }

        verifyFilesHaveSameContent( comparisonDirectory, workingDirectory );
    }

    private void verifyFilesHaveSameContent( File original, File other ) throws IOException
    {
        for ( File originalFile : original.listFiles() )
        {
            File otherFile = new File( other, originalFile.getName() );
            if ( !originalFile.isDirectory() )
            {
                BufferedInputStream originalStream = new BufferedInputStream( new FileInputStream( originalFile ) );
                BufferedInputStream otherStream = new BufferedInputStream( new FileInputStream( otherFile ) );

                int aByte;
                while ( (aByte = originalStream.read()) != -1 )
                {
                    assertEquals( "Different content in " + originalFile.getName(), aByte, otherStream.read() );
                }

                originalStream.close();
                otherStream.close();
            }
        }
    }

    private static class AlwaysAllowedUpgradeConfiguration implements UpgradeConfiguration
    {
        public void checkConfigurationAllowsAutomaticUpgrade()
        {
        }
    }
}

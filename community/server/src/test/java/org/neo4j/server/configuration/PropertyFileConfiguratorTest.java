/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.server.configuration;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.logging.BufferingConsoleLogger;
import org.neo4j.test.Mute;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.neo4j.test.Mute.muteAll;

public class PropertyFileConfiguratorTest
{
    @Rule
    public final Mute mute = muteAll();
    @Rule
    public final TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void whenDatabaseTuningFilePresentInDefaultLocationShouldLoadItEvenIfNotSpecified() throws IOException
    {
        File emptyPropertyFile = PropertyFileBuilder.builder( folder.getRoot() )
                .build();
        DatabaseTuningPropertyFileBuilder.builder( folder.getRoot() )
                .build();

        PropertyFileConfigurator configurator = new PropertyFileConfigurator( emptyPropertyFile );

        assertEquals( "25M", configurator.getDatabaseTuningProperties()
                .get( GraphDatabaseSettings.nodestore_mapped_memory_size.name() ) );
    }

    @Test
    public void whenDatabaseTuningFilePresentInDefaultLocationShouldNotLoadIfAnotherSpecified() throws IOException
    {
        int unlikelyDefaultMemoryMappedValue = 8351;
        File databaseTuningPropertyFileWeWantToUse = DatabaseTuningPropertyFileBuilder.builder( folder.getRoot() )
                .mappedMemory( unlikelyDefaultMemoryMappedValue )
                .build();
        File emptyPropertyFile = PropertyFileBuilder.builder( folder.getRoot() )
                .withDbTuningPropertyFile( databaseTuningPropertyFileWeWantToUse )
                .build();
        // The tuning properties we want to ignore, in the same dir as the neo
        // server properties
        DatabaseTuningPropertyFileBuilder.builder( folder.newFolder() )
                .build();

        PropertyFileConfigurator configurator = new PropertyFileConfigurator( emptyPropertyFile );

        assertEquals( String.valueOf( unlikelyDefaultMemoryMappedValue ) + "M",
                configurator.getDatabaseTuningProperties()
                        .get( GraphDatabaseSettings.nodestore_mapped_memory_size.name() ) );
    }

    @Test
    public void shouldLogInfoWhenDefaultingToTuningPropertiesFileInTheSameDirectoryAsTheNeoServerPropertiesFile()
            throws IOException
    {
        File emptyPropertyFile = PropertyFileBuilder.builder( folder.getRoot() )
                .build();
        File tuningPropertiesFile = DatabaseTuningPropertyFileBuilder.builder( folder.getRoot() )
                .build();

        BufferingConsoleLogger logger = new BufferingConsoleLogger();
        new PropertyFileConfigurator( emptyPropertyFile, logger );

        assertThat( logger.toString(), containsString( String.format(
                "No database tuning file explicitly set, defaulting to [%s]",
                tuningPropertiesFile.getAbsolutePath() ) ) );
    }

    @Test
    public void shouldRetainRegistrationOrderOfThirdPartyJaxRsPackages() throws IOException
    {
        File propertyFile = PropertyFileBuilder.builder( folder.getRoot() )
                .withNameValue( Configurator.THIRD_PARTY_PACKAGES_KEY,
                        "org.neo4j.extension.extension1=/extension1,org.neo4j.extension.extension2=/extension2," +
                        "org.neo4j.extension.extension3=/extension3" )
                .build();
        PropertyFileConfigurator propertyFileConfigurator = new PropertyFileConfigurator( propertyFile );

        List<ThirdPartyJaxRsPackage> thirdpartyJaxRsPackages =
                propertyFileConfigurator.configuration().get( ServerSettings.third_party_packages );

        assertEquals( 3, thirdpartyJaxRsPackages.size() );
        assertEquals( "/extension1", thirdpartyJaxRsPackages.get( 0 ).getMountPoint() );
        assertEquals( "/extension2", thirdpartyJaxRsPackages.get( 1 ).getMountPoint() );
        assertEquals( "/extension3", thirdpartyJaxRsPackages.get( 2 ).getMountPoint() );

    }

    @Test
    public void shouldSetStoreDirSetting() throws Exception
    {
        // Given
        String dbLocation = new File( "/tmp/does_not_matter" ).getAbsolutePath();
        File propertyFile = PropertyFileBuilder.builder( folder.getRoot() )
                .withNameValue( Configurator.DATABASE_LOCATION_PROPERTY_KEY, dbLocation ).build();
        PropertyFileConfigurator serverConfig = new PropertyFileConfigurator( propertyFile );

        // When
        Map<String,String> properties = serverConfig.getDatabaseTuningProperties();

        // Then
        assertThat( properties.get( GraphDatabaseSettings.store_dir.name() ), equalTo( dbLocation ) );
    }

    @Test
    public void shouldWorkFineWithNoPropertiesFile()
    {
        // Given
        File propertiesFile = null;

        // When
        PropertyFileConfigurator configurator = new PropertyFileConfigurator( propertiesFile );

        // Then
        assertFalse( configurator.getDatabaseTuningProperties().isEmpty() );
    }

    @Test
    public void shouldWorkFineWhenSpecifiedPropertiesFileDoesNotExist()
    {
        // Given
        File nonExistentFilePropertiesFile = new File( "/tmp/" + System.currentTimeMillis() );

        // When
        PropertyFileConfigurator configurator = new PropertyFileConfigurator( nonExistentFilePropertiesFile );

        // Then
        assertFalse( configurator.getDatabaseTuningProperties().isEmpty() );
    }
}

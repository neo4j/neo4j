/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.Log;
import org.neo4j.test.SuppressOutput;
import org.neo4j.logging.NullLog;

import static org.junit.Assert.assertEquals;
import static org.neo4j.test.SuppressOutput.suppressAll;

import static org.junit.Assert.assertNotNull;

public class BaseServerConfigLoaderTest
{
    @Rule
    public final SuppressOutput suppressOutput = suppressAll();
    @Rule
    public final TemporaryFolder folder = new TemporaryFolder();

    private final Log log = NullLog.getInstance();
    private final BaseServerConfigLoader configLoader = new BaseServerConfigLoader();

    @Test
    public void whenDatabaseTuningFilePresentInDefaultLocationShouldLoadItEvenIfNotSpecified() throws IOException
    {
        // given
        File emptyPropertyFile = PropertyFileBuilder.builder( folder.getRoot() )
                .build();
        DatabaseTuningPropertyFileBuilder.builder( folder.getRoot() )
                .withKernelId( "fromdefaultlocation" )
                .build();

        // when
        Config config = configLoader.loadConfig( null, emptyPropertyFile, log );

        // then
        assertEquals( "fromdefaultlocation", config.get( GraphDatabaseSettings.forced_kernel_id ) );
    }

    @Test
    public void whenDatabaseTuningFilePresentInDefaultLocationShouldNotLoadIfAnotherSpecified() throws IOException
    {
        // given
        File databaseTuningPropertyFileWeWantToUse = DatabaseTuningPropertyFileBuilder.builder( folder.getRoot() )
                .withKernelId( "shouldgetloaded" )
                .build();
        File emptyPropertyFile = PropertyFileBuilder.builder( folder.getRoot() )
                .withDbTuningPropertyFile( databaseTuningPropertyFileWeWantToUse )
                .build();
        // The tuning properties we want to ignore, in the same dir as the neo
        // server properties
        DatabaseTuningPropertyFileBuilder.builder( folder.newFolder() )
                .withKernelId( "shouldnotgetloaded" )
                .build();

        // when
        Config config = configLoader.loadConfig( null, emptyPropertyFile, log );

        // then
        assertEquals( "shouldgetloaded", config.get( GraphDatabaseSettings.forced_kernel_id ) );
    }

    @Test
    public void shouldRetainRegistrationOrderOfThirdPartyJaxRsPackages() throws IOException
    {
        // given
        File propertyFile = PropertyFileBuilder.builder( folder.getRoot() )
                .withNameValue( Configurator.THIRD_PARTY_PACKAGES_KEY,
                        "org.neo4j.extension.extension1=/extension1,org.neo4j.extension.extension2=/extension2," +
                        "org.neo4j.extension.extension3=/extension3" )
                .build();

        // when
        Config config = configLoader.loadConfig( null, propertyFile, log );

        // then
        List<ThirdPartyJaxRsPackage> thirdpartyJaxRsPackages = config.get( ServerSettings.third_party_packages );

        assertEquals( 3, thirdpartyJaxRsPackages.size() );
        assertEquals( "/extension1", thirdpartyJaxRsPackages.get( 0 ).getMountPoint() );
        assertEquals( "/extension2", thirdpartyJaxRsPackages.get( 1 ).getMountPoint() );
        assertEquals( "/extension3", thirdpartyJaxRsPackages.get( 2 ).getMountPoint() );

    }

    @Test
    public void shouldWorkFineWhenSpecifiedPropertiesFileDoesNotExist()
    {
        // Given
        File nonExistentFilePropertiesFile = new File( "/tmp/" + System.currentTimeMillis() );

        // When
        Config config = configLoader.loadConfig( null, nonExistentFilePropertiesFile, log );

        // Then
        assertNotNull( config );
    }
}

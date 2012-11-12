/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.io.IOException;

import org.junit.Test;
import org.neo4j.server.logging.InMemoryAppender;

public class PropertyFileConfiguratorTest
{
    @Test
    public void whenDatabaseTuningFilePresentInDefaultLocationShouldLoadItEvenIfNotSpecified() throws IOException
    {
        File emptyPropertyFile = PropertyFileBuilder.builder()
                .build();
        DatabaseTuningPropertyFileBuilder.builder()
                .inDirectory( emptyPropertyFile.getParentFile() )
                .build();

        PropertyFileConfigurator configurator = new PropertyFileConfigurator( emptyPropertyFile );

        assertNotNull( configurator.getDatabaseTuningProperties()
                .get( "neostore.nodestore.db.mapped_memory" ) );
        assertEquals( "25M", configurator.getDatabaseTuningProperties()
                .get( "neostore.nodestore.db.mapped_memory" ) );
    }

    @Test
    public void whenDatabaseTuningFilePresentInDefaultLocationShouldNotLoadIfAnotherSpecified() throws IOException
    {
        int unlikelyDefaultMemoryMappedValue = 8351;
        File databaseTuningPropertyFileWeWantToUse = DatabaseTuningPropertyFileBuilder.builder()
                .mappedMemory( unlikelyDefaultMemoryMappedValue )
                .build();
        File emptyPropertyFile = PropertyFileBuilder.builder()
                .withDbTuningPropertyFile( databaseTuningPropertyFileWeWantToUse )
                .build();
        // The tuning properties we want to ignore, in the same dir as the neo
        // server properties
        DatabaseTuningPropertyFileBuilder.builder()
                .inDirectory( emptyPropertyFile.getParentFile() )
                .build();

        PropertyFileConfigurator configurator = new PropertyFileConfigurator( emptyPropertyFile );

        assertNotNull( configurator.getDatabaseTuningProperties()
                .get( "neostore.nodestore.db.mapped_memory" ) );
        assertEquals( String.valueOf( unlikelyDefaultMemoryMappedValue ) + "M",
                configurator.getDatabaseTuningProperties()
                        .get( "neostore.nodestore.db.mapped_memory" ) );
    }

    @Test
    public void shouldLogInfoWhenDefaultingToTuningPropertiesFileInTheSameDirectoryAsTheNeoServerPropertiesFile()
            throws IOException
    {
        File emptyPropertyFile = PropertyFileBuilder.builder()
                .build();
        File tuningPropertiesFile = DatabaseTuningPropertyFileBuilder.builder()
                .inDirectory( emptyPropertyFile.getParentFile() )
                .build();

        InMemoryAppender appender = new InMemoryAppender( PropertyFileConfigurator.log );
        new PropertyFileConfigurator( emptyPropertyFile );

        assertThat( appender.toString(), containsString( String.format(
                "INFO: No database tuning file explicitly set, defaulting to [%s]",
                tuningPropertiesFile.getAbsolutePath() ) ) );
    }
}

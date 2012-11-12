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
package org.neo4j.server.startup.healthcheck;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.junit.Test;
import org.neo4j.server.ServerTestUtils;
import org.neo4j.server.configuration.Configurator;

public class ConfigFileMustBePresentRuleTest
{
    @Test
    public void shouldFailIfThereIsNoSystemPropertyForConfigFile()
    {
        ConfigFileMustBePresentRule rule = new ConfigFileMustBePresentRule();
        assertFalse( rule.execute( propertiesWithoutConfigFileLocation() ) );
    }

    @Test
    public void shouldFailIfThereIsNoConfigFileWhereTheSystemPropertyPoints() throws IOException
    {
        ConfigFileMustBePresentRule rule = new ConfigFileMustBePresentRule();
        File tempFile = ServerTestUtils.createTempPropertyFile();
        tempFile.delete();

        assertFalse( rule.execute( propertiesWithConfigFileLocation( tempFile ) ) );
    }

    @Test
    public void shouldPassIfThereIsAConfigFileWhereTheSystemPropertyPoints() throws IOException
    {
        File propertyFile = ServerTestUtils.createTempPropertyFile();
        ServerTestUtils.writePropertyToFile( Configurator.DATABASE_LOCATION_PROPERTY_KEY, "/tmp/foo.db", propertyFile );

        ConfigFileMustBePresentRule rule = new ConfigFileMustBePresentRule();
        assertTrue( rule.execute( propertiesWithConfigFileLocation( propertyFile ) ) );
        propertyFile.delete();
    }

    private Properties propertiesWithoutConfigFileLocation()
    {
        System.clearProperty( Configurator.NEO_SERVER_CONFIG_FILE_KEY );
        return System.getProperties();
    }

    private Properties propertiesWithConfigFileLocation( File propertyFile )
    {
        System.setProperty( Configurator.NEO_SERVER_CONFIG_FILE_KEY, propertyFile.getAbsolutePath() );
        return System.getProperties();
    }
}

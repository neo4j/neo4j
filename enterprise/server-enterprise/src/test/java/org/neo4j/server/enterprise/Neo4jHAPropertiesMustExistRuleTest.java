/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

package org.neo4j.server.enterprise;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import org.junit.Test;
import org.neo4j.kernel.HaConfig;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.server.configuration.Configurator;

import static org.junit.Assert.*;

public class Neo4jHAPropertiesMustExistRuleTest
{
    // TODO: write more tests

    @Test
    public void shouldPassIfHaModeNotSpecified() throws Exception
    {
        File serverPropertyFile = ServerTestUtils.createTempPropertyFile();
        ServerTestUtils.writePropertyToFile( "touch", "me", serverPropertyFile );
        assertRulePass( new Neo4jHAPropertiesMustExistRule(), serverPropertyFile );
    }

    @Test
    public void shouldFailIfInvalidModeSpecified() throws Exception
    {
        File serverPropertyFile = ServerTestUtils.createTempPropertyFile();
        ServerTestUtils.writePropertyToFile( Configurator.DB_MODE_KEY, "faulty", serverPropertyFile );
        assertRuleFail( new Neo4jHAPropertiesMustExistRule(), serverPropertyFile );
    }

    @Test
    public void shouldPassIfHAModeIsSetAndTheDbTuningFileHasBeenSpecifiedAndExists() throws IOException
    {
        Neo4jHAPropertiesMustExistRule rule = new Neo4jHAPropertiesMustExistRule();
        File serverPropertyFile = ServerTestUtils.createTempPropertyFile();
        File dbTuningFile = ServerTestUtils.createTempPropertyFile();
        ServerTestUtils.writePropertyToFile(Configurator.DB_TUNING_PROPERTY_FILE_KEY, dbTuningFile.getAbsolutePath(), serverPropertyFile);
        ServerTestUtils.writePropertyToFile( Configurator.DB_MODE_KEY, "ha", serverPropertyFile );
        ServerTestUtils.writePropertyToFile( HaSettings.server_id.name(), "1", dbTuningFile );
        ServerTestUtils.writePropertyToFile( HaSettings.coordinators.name(),
                "localhost:0000", dbTuningFile );

        assertRulePass( rule, serverPropertyFile );

        serverPropertyFile.delete();
        dbTuningFile.delete();
    }

    @Test
    public void shouldPassIfHAModeIsSetAndTheDbTuningFileHasBeenSpecifiedAndExistsWithOldConfig() throws IOException
    {
        Neo4jHAPropertiesMustExistRule rule = new Neo4jHAPropertiesMustExistRule();
        File serverPropertyFile = ServerTestUtils.createTempPropertyFile();
        File dbTuningFile = ServerTestUtils.createTempPropertyFile();
        ServerTestUtils.writePropertyToFile(Configurator.DB_TUNING_PROPERTY_FILE_KEY, dbTuningFile.getAbsolutePath(), serverPropertyFile);
        ServerTestUtils.writePropertyToFile( Configurator.DB_MODE_KEY, "ha", serverPropertyFile );
        ServerTestUtils.writePropertyToFile( HaConfig.CONFIG_KEY_OLD_SERVER_ID, "1", dbTuningFile );
        ServerTestUtils.writePropertyToFile( HaConfig.CONFIG_KEY_OLD_COORDINATORS,
                "localhost:0000", dbTuningFile );

        assertRulePass( rule, serverPropertyFile );

        serverPropertyFile.delete();
        dbTuningFile.delete();
    }
    
    @Test
    public void shouldFailIfHAModeIsSetAndTheDbTuningFileHasBeenSpecifiedAndExistsWithDuplicateIdConfig() throws IOException
    {
        Neo4jHAPropertiesMustExistRule rule = new Neo4jHAPropertiesMustExistRule();
        File serverPropertyFile = ServerTestUtils.createTempPropertyFile();
        File dbTuningFile = ServerTestUtils.createTempPropertyFile();
        ServerTestUtils.writePropertyToFile(Configurator.DB_TUNING_PROPERTY_FILE_KEY, dbTuningFile.getAbsolutePath(), serverPropertyFile);
        ServerTestUtils.writePropertyToFile( Configurator.DB_MODE_KEY, "ha", serverPropertyFile );
        ServerTestUtils.writePropertyToFile( HaConfig.CONFIG_KEY_OLD_SERVER_ID, "1", dbTuningFile );
        ServerTestUtils.writePropertyToFile( HaSettings.server_id.name(), "1", dbTuningFile );
        ServerTestUtils.writePropertyToFile( HaSettings.coordinators.name(),
                "localhost:0000", dbTuningFile );

        assertRuleFail( rule, serverPropertyFile );

        serverPropertyFile.delete();
        dbTuningFile.delete();
    }
    
    @Test
    public void shouldFailIfHAModeIsSetAndTheDbTuningFileHasBeenSpecifiedAndExistsWithDuplicateCoordinatorConfig() throws IOException
    {
        Neo4jHAPropertiesMustExistRule rule = new Neo4jHAPropertiesMustExistRule();
        File serverPropertyFile = ServerTestUtils.createTempPropertyFile();
        File dbTuningFile = ServerTestUtils.createTempPropertyFile();
        ServerTestUtils.writePropertyToFile(Configurator.DB_TUNING_PROPERTY_FILE_KEY, dbTuningFile.getAbsolutePath(), serverPropertyFile);
        ServerTestUtils.writePropertyToFile( Configurator.DB_MODE_KEY, "ha", serverPropertyFile );
        ServerTestUtils.writePropertyToFile( HaSettings.server_id.name(), "1", dbTuningFile );
        ServerTestUtils.writePropertyToFile( HaSettings.coordinators.name(),
                "localhost:0000", dbTuningFile );
        ServerTestUtils.writePropertyToFile( HaConfig.CONFIG_KEY_OLD_COORDINATORS,
                "localhost:0000", dbTuningFile );

        assertRuleFail( rule, serverPropertyFile );

        serverPropertyFile.delete();
        dbTuningFile.delete();
    }
    
    @Test
    public void shouldFailIfHAModeIsSetAndTheDbTuningFileHasBeenSpecifiedButDoesNotExist() throws IOException
    {
        Neo4jHAPropertiesMustExistRule rule = new Neo4jHAPropertiesMustExistRule();
        File serverPropertyFile = ServerTestUtils.createTempPropertyFile();
        File dbTuningFile = ServerTestUtils.createTempPropertyFile();
        ServerTestUtils.writePropertyToFile(Configurator.DB_TUNING_PROPERTY_FILE_KEY, dbTuningFile.getAbsolutePath(), serverPropertyFile);
        ServerTestUtils.writePropertyToFile( Configurator.DB_MODE_KEY, "ha", serverPropertyFile );

        assertRuleFail( rule, serverPropertyFile );

        serverPropertyFile.delete();
        dbTuningFile.delete();
    }

    @Test
    public void shouldFailIfHAModeIsSetAndTheDbTuningFileHasNotBeenSpecified() throws IOException
    {
        Neo4jHAPropertiesMustExistRule rule = new Neo4jHAPropertiesMustExistRule();
        File serverPropertyFile = ServerTestUtils.createTempPropertyFile();
        ServerTestUtils.writePropertyToFile( Configurator.DB_MODE_KEY, "ha", serverPropertyFile );

        assertRuleFail( rule, serverPropertyFile );

        serverPropertyFile.delete();
    }

    private void assertRulePass( Neo4jHAPropertiesMustExistRule rule, File propFile )
    {
        if ( !rule.execute( propertiesWithConfigFileLocation( propFile ) ) )
        {
            fail( rule.getFailureMessage() );
        }
    }

    private void assertRuleFail( Neo4jHAPropertiesMustExistRule rule, File propFile )
    {
        if ( rule.execute( propertiesWithConfigFileLocation( propFile ) ) )
        {
            fail( rule + " should have failed" );
        }
    }
    
    private Properties propertiesWithConfigFileLocation( File propertyFile )
    {
        Properties result = new Properties();
        result.setProperty( Configurator.NEO_SERVER_CONFIG_FILE_KEY, propertyFile.getAbsolutePath() );
        return result;
    }
}

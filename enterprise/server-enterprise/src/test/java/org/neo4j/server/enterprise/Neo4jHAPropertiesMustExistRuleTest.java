/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.apache.commons.configuration.Configuration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.configuration.PropertyFileConfigurator;

import static org.junit.Assert.fail;

import static org.neo4j.kernel.logging.ConsoleLogger.DEV_NULL;
import static org.neo4j.server.configuration.validation.Validator.NO_VALIDATION;

public class Neo4jHAPropertiesMustExistRuleTest
{
    @Rule
    public TemporaryFolder folder = new TemporaryFolder(  );

    public static final String CONFIG_KEY_OLD_SERVER_ID = "ha.machine_id";
    public static final String CONFIG_KEY_OLD_COORDINATORS = "ha.zoo_keeper_servers";

    // TODO: write more tests

    @Test
    public void shouldPassIfHaModeNotSpecified() throws Exception
    {
        File serverPropertyFile = ServerTestUtils.createTempPropertyFile(folder.getRoot());
        ServerTestUtils.writePropertyToFile( "touch", "me", serverPropertyFile );
        assertRulePass( serverPropertyFile );
    }

    @Test
    public void shouldFailIfInvalidModeSpecified() throws Exception
    {
        File serverPropertyFile = ServerTestUtils.createTempPropertyFile(folder.getRoot());
        ServerTestUtils.writePropertyToFile( Configurator.DB_MODE_KEY, "faulty", serverPropertyFile );
        assertRuleFail( serverPropertyFile );
    }

    @Test
    public void shouldPassIfHAModeIsSetAndTheDbTuningFileHasBeenSpecifiedAndExists() throws IOException
    {
        File serverPropertyFile = ServerTestUtils.createTempPropertyFile(folder.getRoot());
        File dbTuningFile = ServerTestUtils.createTempPropertyFile(folder.getRoot());
        ServerTestUtils.writePropertyToFile( Configurator.DB_TUNING_PROPERTY_FILE_KEY,
                dbTuningFile.getAbsolutePath(), serverPropertyFile );
        ServerTestUtils.writePropertyToFile( Configurator.DB_MODE_KEY, "ha", serverPropertyFile );
        ServerTestUtils.writePropertyToFile( ClusterSettings.server_id.name(), "1", dbTuningFile );

        assertRulePass( serverPropertyFile );

        serverPropertyFile.delete();
        dbTuningFile.delete();
    }

    @Test
    public void shouldPassIfHAModeIsSetAndTheDbTuningFileHasBeenSpecifiedAndExistsWithOldConfig() throws IOException
    {
        File serverPropertyFile = ServerTestUtils.createTempPropertyFile(folder.getRoot());
        File dbTuningFile = ServerTestUtils.createTempPropertyFile(folder.getRoot());
        ServerTestUtils.writePropertyToFile( Configurator.DB_TUNING_PROPERTY_FILE_KEY,
                dbTuningFile.getAbsolutePath(), serverPropertyFile );
        ServerTestUtils.writePropertyToFile( Configurator.DB_MODE_KEY, "ha", serverPropertyFile );
        ServerTestUtils.writePropertyToFile( CONFIG_KEY_OLD_SERVER_ID, "1", dbTuningFile );
        ServerTestUtils.writePropertyToFile( CONFIG_KEY_OLD_COORDINATORS,
                "localhost:0000", dbTuningFile );

        assertRulePass( serverPropertyFile );

        serverPropertyFile.delete();
        dbTuningFile.delete();
    }

    @Test
    public void shouldFailIfHAModeIsSetAndTheDbTuningFileHasBeenSpecifiedAndExistsWithDuplicateIdConfig() throws
            IOException
    {
        File serverPropertyFile = ServerTestUtils.createTempPropertyFile(folder.getRoot());
        File dbTuningFile = ServerTestUtils.createTempPropertyFile(folder.getRoot());
        ServerTestUtils.writePropertyToFile( Configurator.DB_TUNING_PROPERTY_FILE_KEY,
                dbTuningFile.getAbsolutePath(), serverPropertyFile );
        ServerTestUtils.writePropertyToFile( Configurator.DB_MODE_KEY, "ha", serverPropertyFile );
        ServerTestUtils.writePropertyToFile( CONFIG_KEY_OLD_SERVER_ID, "1", dbTuningFile );
        ServerTestUtils.writePropertyToFile( ClusterSettings.server_id.name(), "1", dbTuningFile );

        assertRuleFail( serverPropertyFile );

        serverPropertyFile.delete();
        dbTuningFile.delete();
    }

    @Test
    public void shouldFailIfHAModeIsSetAndTheDbTuningFileHasBeenSpecifiedButDoesNotExist() throws IOException
    {
        File serverPropertyFile = ServerTestUtils.createTempPropertyFile(folder.getRoot());
        File dbTuningFile = ServerTestUtils.createTempPropertyFile(folder.getRoot());
        ServerTestUtils.writePropertyToFile( Configurator.DB_TUNING_PROPERTY_FILE_KEY,
                dbTuningFile.getAbsolutePath(), serverPropertyFile );
        ServerTestUtils.writePropertyToFile( Configurator.DB_MODE_KEY, "ha", serverPropertyFile );

        assertRuleFail( serverPropertyFile );

        serverPropertyFile.delete();
        dbTuningFile.delete();
    }

    @Test
    public void shouldFailIfHAModeIsSetAndTheDbTuningFileHasNotBeenSpecified() throws IOException
    {
        File serverPropertyFile = ServerTestUtils.createTempPropertyFile(folder.getRoot());
        ServerTestUtils.writePropertyToFile( Configurator.DB_MODE_KEY, "ha", serverPropertyFile );

        assertRuleFail( serverPropertyFile );

        serverPropertyFile.delete();
    }

    private void assertRulePass( File file )
    {
        EnsureEnterpriseNeo4jPropertiesExist rule = new EnsureEnterpriseNeo4jPropertiesExist(
                propertiesWithConfigFileLocation( file ) );
        if ( !rule.run() )
        {
            fail( rule.getFailureMessage() );
        }
    }

    private void assertRuleFail( File file )
    {
        EnsureEnterpriseNeo4jPropertiesExist rule = new EnsureEnterpriseNeo4jPropertiesExist(
                propertiesWithConfigFileLocation( file ) );
        if ( rule.run() )
        {
            fail( rule + " should have failed" );
        }
    }

    private Configuration propertiesWithConfigFileLocation( File propertyFile )
    {
        PropertyFileConfigurator config = new PropertyFileConfigurator( NO_VALIDATION, propertyFile, DEV_NULL );
        config.configuration().setProperty( Configurator.NEO_SERVER_CONFIG_FILE_KEY, propertyFile.getAbsolutePath() );
        return config.configuration();
    }
}

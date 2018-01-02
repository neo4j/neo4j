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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.Log;
import org.neo4j.logging.NullLog;
import org.neo4j.server.ServerTestUtils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ServerConfigLoaderTest
{
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private final Log log = NullLog.getInstance();
    private final BaseServerConfigLoader configLoader = new BaseServerConfigLoader();

    @Test
    public void shouldProvideAConfiguration() throws IOException
    {
        // given
        File configFile = PropertyFileBuilder.builder( folder.getRoot() )
                .build();

        // when
        Config config = configLoader.loadConfig( null, configFile, log );

        // then
        assertNotNull( config );
    }

    @Test
    public void shouldUseSpecifiedConfigFile() throws Exception
    {
        // given
        File configFile = PropertyFileBuilder.builder( folder.getRoot() )
                .withNameValue( "foo", "bar" )
                .build();

        // when
        Config testConf = configLoader.loadConfig( null, configFile, log );

        // then
        final String EXPECTED_VALUE = "bar";
        assertEquals( EXPECTED_VALUE, testConf.getParams().get( "foo" ) );
    }

    @Test
    public void shouldAcceptDuplicateKeysWithSameValue() throws IOException
    {
        // given
        File configFile = PropertyFileBuilder.builder( folder.getRoot() )
                .withNameValue( "foo", "bar" )
                .withNameValue( "foo", "bar" )
                .build();

        // when
        Config testConf = configLoader.loadConfig( null, configFile, log );

        // then
        assertNotNull( testConf );
        final String EXPECTED_VALUE = "bar";
        assertEquals( EXPECTED_VALUE, testConf.getParams().get( "foo" ) );
    }

    @Test
    public void shouldSupportProvidingDatabaseTuningParametersSeparately() throws IOException
    {
        // given
        File databaseTuningPropertyFile = DatabaseTuningPropertyFileBuilder.builder( folder.getRoot() )
                .withKernelId( "kernelfromseparatedbtuningfile" )
                .build();
        File propertyFileWithDbTuningProperty = PropertyFileBuilder.builder( folder.getRoot() )
                .withDbTuningPropertyFile( databaseTuningPropertyFile )
                .build();

        // when
        Config config = configLoader.loadConfig( null, propertyFileWithDbTuningProperty, log );

        // then
        assertNotNull( config );
        assertEquals( config.get( GraphDatabaseSettings.forced_kernel_id ), "kernelfromseparatedbtuningfile" );
    }

    @Test
    public void shouldFindThirdPartyJaxRsPackages() throws IOException
    {
        // given
        File file = ServerTestUtils.createTempPropertyFile( folder.getRoot() );

        try(BufferedWriter out = new BufferedWriter( new FileWriter( file, true ) ))
        {
            out.write( Configurator.THIRD_PARTY_PACKAGES_KEY );
            out.write( "=" );
            out.write( "com.foo.bar=\"mount/point/foo\"," );
            out.write( "com.foo.baz=\"/bar\"," );
            out.write( "com.foo.foobarbaz=\"/\"" );
            out.write( System.lineSeparator() );
        }

        // when
        Config config = configLoader.loadConfig( null, file, log );

        // then
        List<ThirdPartyJaxRsPackage> thirdpartyJaxRsPackages = config.get( ServerSettings.third_party_packages );
        assertNotNull( thirdpartyJaxRsPackages );
        assertEquals( 3, thirdpartyJaxRsPackages.size() );
    }
}

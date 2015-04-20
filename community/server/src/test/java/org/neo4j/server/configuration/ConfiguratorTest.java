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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.server.ServerTestUtils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ConfiguratorTest
{
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void shouldProvideAConfiguration() throws IOException
    {
        File configFile = PropertyFileBuilder.builder( folder.getRoot() )
                .build();
        Config config = new PropertyFileConfigurator( configFile ).configuration();
        assertNotNull( config );
    }

    @Test
    public void shouldUseSpecifiedConfigFile() throws Exception
    {
        File configFile = PropertyFileBuilder.builder( folder.getRoot() )
                .withNameValue( "foo", "bar" )
                .build();

        Config testConf = new PropertyFileConfigurator( configFile ).configuration();

        final String EXPECTED_VALUE = "bar";
        assertEquals( EXPECTED_VALUE, testConf.getParams().get( "foo" ) );
    }

    @Test
    public void shouldAcceptDuplicateKeysWithSameValue() throws IOException
    {
        File configFile = PropertyFileBuilder.builder( folder.getRoot() )
                .withNameValue( "foo", "bar" )
                .withNameValue( "foo", "bar" )
                .build();

        ConfigurationBuilder configurator = new PropertyFileConfigurator( configFile );
        Config testConf = configurator.configuration();

        assertNotNull( testConf );
        final String EXPECTED_VALUE = "bar";
        assertEquals( EXPECTED_VALUE, testConf.getParams().get( "foo" ) );
    }

    @Test
    public void shouldSupportProvidingDatabaseTuningParametersSeparately() throws IOException
    {
        File databaseTuningPropertyFile = DatabaseTuningPropertyFileBuilder.builder( folder.getRoot() )
                .build();

        File propertyFileWithDbTuningProperty = PropertyFileBuilder.builder( folder.getRoot() )
                .withDbTuningPropertyFile( databaseTuningPropertyFile )
                .build();

        ConfigurationBuilder configurator = new PropertyFileConfigurator( propertyFileWithDbTuningProperty );

        Map<String, String> databaseTuningProperties = configurator.getDatabaseTuningProperties();
        assertNotNull( databaseTuningProperties );
        assertEquals( 6, databaseTuningProperties.size() );
    }

    @Test
    public void shouldFindThirdPartyJaxRsPackages() throws IOException
    {
        File file = ServerTestUtils.createTempPropertyFile( folder.getRoot() );

        FileWriter fstream = new FileWriter( file, true );
        BufferedWriter out = new BufferedWriter( fstream );
        out.write( Configurator.THIRD_PARTY_PACKAGES_KEY );
        out.write( "=" );
        out.write( "com.foo.bar=\"mount/point/foo\"," );
        out.write( "com.foo.baz=\"/bar\"," );
        out.write( "com.foo.foobarbaz=\"/\"" );
        out.write( System.getProperty( "line.separator" ) );
        out.close();

        ConfigurationBuilder configurator = new PropertyFileConfigurator( file );

        List<ThirdPartyJaxRsPackage> thirdpartyJaxRsPackages = configurator.configuration().get( ServerSettings.third_party_packages );
        assertNotNull( thirdpartyJaxRsPackages );
        assertEquals( 3, thirdpartyJaxRsPackages.size() );
    }
}

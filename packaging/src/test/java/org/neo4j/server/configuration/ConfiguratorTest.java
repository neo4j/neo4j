/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.server.configuration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.apache.commons.configuration.Configuration;
import org.junit.Test;
import org.neo4j.server.ServerTestUtils;
import org.neo4j.server.configuration.validation.Validator;

public class ConfiguratorTest {

    @Test
    public void shouldProvideAConfiguration() throws IOException {
        File configFile = PropertyFileBuilder.builder().build();
        Configuration config = new Configurator(new Validator(), configFile).configuration();
        assertNotNull(config);
    }

    @Test
    public void shouldUseSpecifiedConfigFile() throws Exception {

        File configFile = PropertyFileBuilder.builder().withNameValue("foo", "bar").build();

        Configuration testConf = new Configurator(new Validator(), configFile).configuration();

        final String EXPECTED_VALUE = "bar";
        assertEquals(EXPECTED_VALUE, testConf.getString("foo"));
    }

    @Test
    public void shouldAcceptDuplicateKeysWithSameValue() throws IOException {
        File configFile = PropertyFileBuilder.builder().withNameValue("foo", "bar").withNameValue("foo", "bar").build();

        Configurator configurator = new Configurator(configFile);
        Configuration testConf = configurator.configuration();

        assertNotNull(testConf);
        final String EXPECTED_VALUE = "bar";
        assertEquals(EXPECTED_VALUE, testConf.getString("foo"));
    }

    private static final String NEOSTORE_NODESTORE_DB_MAPPED_MEMORY_KEY = "neostore.nodestore.db.mapped_memory";
    private static final String NEOSTORE_NODESTORE_DB_MAPPED_MEMORY = "25M";
    private static final String NEOSTORE_RELATIONSHIPSTORE_DB_MAPPED_MEMORY_KEY = "neostore.relationshipstore.db.mapped_memory";
    private static final String NEOSTORE_RELATIONSHIPSTORE_DB_MAPPED_MEMORY = "50M";

    @Test
    public void shouldProvideDatabaseTuningParametersSeparately() throws IOException {
        File databaseTuningPropertyFile = ServerTestUtils.createTempPropertyFile();
        ServerTestUtils.writePropertyToFile(NEOSTORE_NODESTORE_DB_MAPPED_MEMORY_KEY, NEOSTORE_NODESTORE_DB_MAPPED_MEMORY, databaseTuningPropertyFile);
        ServerTestUtils.writePropertyToFile(NEOSTORE_RELATIONSHIPSTORE_DB_MAPPED_MEMORY_KEY, NEOSTORE_RELATIONSHIPSTORE_DB_MAPPED_MEMORY,
                databaseTuningPropertyFile);

        File propertyFileWithDbTuningProperty = PropertyFileBuilder.builder().withDbTuningPropertyFile(databaseTuningPropertyFile).build();

        Configurator configurator = new Configurator(propertyFileWithDbTuningProperty);

        Map<String, String> databaseTuningProperties = configurator.getDatabaseTuningProperties();
        assertNotNull(databaseTuningProperties);
        assertEquals(2, databaseTuningProperties.size());
    }

    @Test
    public void shouldFindThirdPartyJaxRsClasses() throws IOException {

        File file = ServerTestUtils.createTempPropertyFile();

        FileWriter fstream = new FileWriter(file, true);
        BufferedWriter out = new BufferedWriter(fstream);
        out.write(Configurator.THIRD_PARTY_PACKAGES_KEY);
        out.write("=");
        out.write("com.foo.bar=\"mount/point/foo\",");
        out.write("com.foo.baz=\"/bar\",");
        out.write("com.foo.foobarbaz=\"/\"");
        out.write(System.getProperty("line.separator"));
        out.close();

        Configurator configurator = new Configurator(file);

        Set<ThirdPartyJaxRsPackage> thirdpartyJaxRsClasses = configurator.getThirdpartyJaxRsClasses();
        assertNotNull(thirdpartyJaxRsClasses);
        assertEquals(3, thirdpartyJaxRsClasses.size());
    }
}

/**
 * Copyright (c) 2002-2010 "Neo Technology,"
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
import java.util.Random;

import org.apache.commons.configuration.Configuration;
import org.junit.Test;

public class ConfiguratorTest {
    private Random rnd = new Random();

    @Test
    public void shouldProvideAConfiguration() {
        Configuration config = new Configurator().configuration();
        assertNotNull(config);
    }
    
    @Test
    public void shouldUseSpecifiedConfigDir() throws Exception {
        File configDir = createTempDir();
        File configFile = createTempPropertyFile(configDir);
        
        FileWriter fstream = new FileWriter(configFile);
        BufferedWriter out = new BufferedWriter(fstream);
        out.write("org.neo4j.foo=bar");
        out.close();
        
        Configuration testConf = new Configurator(configDir).configuration();

        final String EXPECTED_VALUE = "bar";
        assertEquals(EXPECTED_VALUE, testConf.getString("org.neo4j.foo"));
    }

    @Test
    public void shouldAcceptDuplicateKeysWithSameValue() throws IOException {
        File configDir = createTempDir();
        File configA = createTempPropertyFile(configDir);
        File configB = createTempPropertyFile(configDir);

        BufferedWriter writer = new BufferedWriter(new FileWriter(configA));
        writer.write("foo=bar");
        writer.close();

        writer = new BufferedWriter(new FileWriter(configB));
        writer.write("foo=bar");
        writer.close();

        Configurator configurator = new Configurator(configDir);
        Configuration testConf = configurator.configuration();

        assertNotNull(testConf);
    }

    @Test(expected = InvalidServerConfigurationException.class)
    public void shouldFailOnDuplicateKeysWithDifferentValues() throws IOException {

        File configDir = createTempDir();
        File configA = createTempPropertyFile(configDir);
        File configB = createTempPropertyFile(configDir);

        BufferedWriter writer = new BufferedWriter(new FileWriter(configA));
        writer.write("foo=bar");
        writer.close();

        writer = new BufferedWriter(new FileWriter(configB));
        writer.write("foo=differentBar");
        writer.close();

        new Configurator(configDir);
    }

    private File createTempDir() throws IOException {
        File d = File.createTempFile("neo4j-test", "dir");
        if (!d.delete())
            throw new RuntimeException("temp config directory pre-delete failed");
        if (!d.mkdirs())
            throw new RuntimeException("temp config directory not created");
        d.deleteOnExit();
        return d;
    }

    private File createTempPropertyFile(File parentDir) throws IOException {
        File f = new File(parentDir, "test-" + rnd.nextInt() + ".properties");
        f.deleteOnExit();
        return f;
    }
}
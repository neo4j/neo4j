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
package org.neo4j.release.it.std.exec;

import org.junit.Test;
import org.ops4j.pax.runner.platform.DefaultJavaRunner;
import org.ops4j.pax.runner.platform.PlatformException;

import java.io.File;
import java.util.Properties;

/**
 *
 */
public class PaxRunnerTest {

    @Test
    public void shouldRunJava() throws PlatformException {
        boolean waitForExit = true;
        DefaultJavaRunner runner = new DefaultJavaRunner( waitForExit );

        String[] vmOptions = { "-server" };
        String[] classpath = System.getProperty("java.class.path").split(":");
        String mainClass = "org.neo4j.release.it.std.exec.HelloWorldApp";
        String[] programOptions = null;
        String javaHome = System.getProperty("java.home");
        File workingDir = null; // new File(".");

        dumpProperties(System.getProperties());
        
        runner.exec(vmOptions, classpath, mainClass, programOptions, javaHome, workingDir);

    }

    private void dumpProperties(Properties properties) {
        System.out.println("System Properties...");
        for (Object key : properties.keySet()) {
            System.out.println("\t" + key + " = " + properties.getProperty((String) key));
        }
    }
}

/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.io.proc;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

/**
 * Utility methods for accessing information about the current Java process.
 */
public class ProcessUtil
{
    private ProcessUtil()
    {
    }

    /**
     * Get the path to the {@code java} executable that is running this Java program.
     * <p>
     * This is useful for starting other Java programs using the same exact version of Java.
     * <p>
     * This value is computed from the {@code java.home} system property.
     *
     * @return The path to the {@code java} executable that launched this Java process.
     */
    public static Path getJavaExecutable()
    {
        String javaHome = System.getProperty( "java.home" );
        return Paths.get( javaHome, "bin", "java" );
    }

    /**
     * Get the list of command line arguments that were passed to the Java runtime, as opposed to the Java program.
     *
     * @see RuntimeMXBean#getInputArguments()
     * @return The list of arguments, as Strings, that were given to the Java runtime.
     */
    public static List<String> getJavaExecutableArguments()
    {
        RuntimeMXBean runtimeMxBean = ManagementFactory.getRuntimeMXBean();
        return runtimeMxBean.getInputArguments();
    }

    /**
     * Get the current classpath as a list of file names.
     * @return The list of file names that makes the classpath.
     */
    public static List<String> getClassPathList()
    {
        return Arrays.asList( getClassPath().split( File.pathSeparator ) );
    }

    /**
     * Get the classpath as a single string of all the classpath file entries, separated by the path separator.
     *
     * This is based on the {@code java.class.path} system property.
     * @see File#pathSeparator
     * @return The current classpath.
     */
    public static String getClassPath()
    {
        return System.getProperty( "java.class.path" );
    }
}

/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.test.proc;

import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;

/**
 * Utility methods for accessing information about the current Java process.
 */
public final class ProcessUtil
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

    /**
     * Get additional export/open module options that is required to be able to start neo4j as a java process
     * and defined by neo4j own {@code jdk.custom.options} system property.
     *
     * @return array of options that can be passed to the java launcher.
     */
    public static List<String> getModuleOptions()
    {
        var moduleOptions = System.getProperty( "jdk.custom.options" );
        if ( StringUtils.isEmpty( moduleOptions ) )
        {
            return emptyList();
        }
        return Arrays.stream( moduleOptions.split( " " ) ).filter( StringUtils::isNotBlank ).map( String::trim ).collect( Collectors.toList() );
    }

    /**
     * Start java process with java that is defined in {@code java.home}, with classpath that is defined by {@code java.class.path} and
     * with additional module options defined by {@code jdk.custom.options} system property and with provided additional arguments.
     * By default, new process started with inherited io option.
     * @param arguments additional arguments that should be passed to new process
     * @return newly started java process
     */
    public static Process start( String... arguments ) throws IOException
    {
        return start( ProcessBuilder::inheritIO, arguments );
    }

    /**
     * Start java process with java that is defined in {@code java.home}, with classpath that is defined by {@code java.class.path} and
     * with additional module options defined by {@code jdk.custom.options} system property and with provided additional arguments
     * @param configurator process builder additional configurator
     * @param arguments additional arguments that should be passed to new process
     * @return newly started java process
     */
    public static Process start( Consumer<ProcessBuilder> configurator, String... arguments ) throws IOException
    {
        var args = new ArrayList<String>();
        args.add( getJavaExecutable().toString() );
        var moduleOptions = getModuleOptions();
        if ( !moduleOptions.isEmpty() )
        {
            args.addAll( moduleOptions );
        }

        // Classpath can get very long and that can upset Windows, so write it to a file
        Path p = Files.createTempFile( "jvm", ".args" );
        p.toFile().deleteOnExit();
        Files.writeString( p, systemProperties() + " -cp " + getClassPath(), StandardCharsets.UTF_8 );

        args.add( "@" + p.normalize() );
        args.addAll( Arrays.asList( arguments ) );
        ProcessBuilder processBuilder = new ProcessBuilder( args );
        configurator.accept( processBuilder );
        return processBuilder.start();
    }

    private static String systemProperties()
    {
        StringBuilder builder = new StringBuilder();
        Properties properties = System.getProperties();
        for ( Map.Entry<Object,Object> entry : properties.entrySet() )
        {
            String name = entry.getKey().toString();
            if ( !isJdkProperty( name ) )
            {
                builder.append( systemProperty( name, entry.getValue().toString() ) );
                builder.append( " " );
            }
        }
        return builder.toString();
    }

    private static boolean isJdkProperty( String name )
    {
        return name.startsWith( "java" ) || name.startsWith( "os" ) || name.startsWith( "sun" ) || name.startsWith( "user" ) || name.startsWith( "line" );
    }

    private static String systemProperty( String key, String value )
    {
        return "-D" + key + "=" + value;
    }
}

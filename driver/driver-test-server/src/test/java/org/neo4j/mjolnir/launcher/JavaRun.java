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
package org.neo4j.mjolnir.launcher;

import java.io.IOException;

import static java.io.File.separator;
import static org.neo4j.helpers.ArrayUtil.union;

/** Utility for running a main class in a separate process */
public class JavaRun
{
    public static Process exec( Class<?> mainClass, String... args ) throws IOException
    {
        Process process = buildProcess(mainClass, args).start();

        // Add a shutdown hook to try and avoid leaking processes. This won't guard against kill -9 stops, but will
        // cover any other exit types.
        stopOnExit( process );

        return process;
    }

    public static ProcessBuilder buildProcess( Class<?> mainClass, String... args ) throws IOException
    {
        String path = System.getProperty("java.home") + separator + "bin" + separator + "java";

        String[] command = union( new String[]{path, "-cp", classpath(), mainClass.getName()}, args );

        return new ProcessBuilder()
                .inheritIO()
                .command( command );
    }

    private static String classpath()
    {
        return System.getProperty("java.class.path");
    }

    private static void stopOnExit( final Process process )
    {
        Runtime.getRuntime().addShutdownHook( new Thread( new Runnable()
        {
            @Override
            public void run()
            {
                process.destroy();
            }
        } ));
    }

}

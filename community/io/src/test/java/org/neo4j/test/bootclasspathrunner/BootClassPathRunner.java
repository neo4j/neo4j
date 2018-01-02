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
package org.neo4j.test.bootclasspathrunner;

import org.junit.runner.Description;
import org.junit.runner.Runner;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.InitializationError;

import java.io.File;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.net.URL;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class BootClassPathRunner extends Runner
{
    private static final String RMI_RUN_NOTIFIER_NAME = "RunNotifier";

    @Target( ElementType.TYPE )
    @Retention( RetentionPolicy.RUNTIME )
    public @interface BootEntryOf
    {
        Class<?> value();
    }

    private final Class<?> testClass;
    private final String classpathEntryToBootWith;

    @SuppressWarnings( "unchecked" )
    public BootClassPathRunner( Class<?> testClass ) throws Exception
    {
        this.testClass = testClass;
        BootEntryOf bootEntryOf = testClass.getAnnotation( BootEntryOf.class );
        Class<?> bootEntryOfClass = bootEntryOf.value();
        URL location = bootEntryOfClass.getProtectionDomain().getCodeSource().getLocation();
        this.classpathEntryToBootWith = location.getPath();
    }

    @Override
    public Description getDescription()
    {
        try
        {
            Runner describingRunner = new BlockJUnit4ClassRunner( testClass );
            return describingRunner.getDescription();
        }
        catch ( InitializationError initializationError )
        {
            throw new RuntimeException( initializationError );
        }
    }

    @Override
    public void run( RunNotifier notifier )
    {
        StringBuilder classpath = buildClassPath();

        try ( RmiServer server = new RmiServer() )
        {
            int port = server.getPort();
            server.export( RMI_RUN_NOTIFIER_NAME, new DelegatingRemoteRunNotifier( notifier ) );
            RuntimeMXBean runtimeMxBean = ManagementFactory.getRuntimeMXBean();
            List<String> arguments = runtimeMxBean.getInputArguments();

            List<String> command = new ArrayList<>();
            command.add( "java" );
            for ( String argument : arguments )
            {
                if ( !argument.startsWith( "-agentlib" ) )
                {
                    command.add( argument );
                }
            }
            command.add( "-ea" );
            command.add( "-Xbootclasspath/a:" + classpathEntryToBootWith );
            command.add( "-cp" );
            command.add( classpath.toString() );
            command.add( getClass().getName() );
            command.add( String.valueOf( port ) );
            command.add( testClass.getName() );

            ProcessBuilder pb = new ProcessBuilder();
            pb.command( command );
            pb.inheritIO();
            Process process = pb.start();
            process.waitFor();
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }

    private StringBuilder buildClassPath()
    {
        Set<String> classpathEntries = new HashSet<>();
        classpathEntries.addAll( Arrays.asList( System.getProperty( "java.class.path" ).split( File.pathSeparator ) ) );
        classpathEntries.remove( classpathEntryToBootWith );
        StringBuilder classpath = new StringBuilder();
        for ( String classpathEntry : classpathEntries )
        {
            classpath.append( classpathEntry );
            classpath.append( File.pathSeparator );
        }
        classpath.setLength( classpath.length() - 1 ); // Cut off the last pathSeparator
        return classpath;
    }

    public static void main( String[] args ) throws Exception
    {
        int rmiPort = Integer.parseInt( args[0] );
        String testClassName = args[1];
        Class<?> testClass = Class.forName( testClassName );

        Registry registry = LocateRegistry.getRegistry( rmiPort );
        RemoteRunNotifier remote = (RemoteRunNotifier) registry.lookup( RMI_RUN_NOTIFIER_NAME );

        Runner runner = new BlockJUnit4ClassRunner( testClass );
        runner.run( new DelegatingRunNotifier( remote ) );
    }
}

/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.util;

import java.io.File;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.neo4j.commandline.admin.AdminTool;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.helpers.ListenSocketAddress;
import org.neo4j.io.proc.ProcessUtil;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.ProcessStreamHandler;
import org.neo4j.test.StreamConsumer;

import static java.lang.String.format;
import static org.neo4j.kernel.configuration.Settings.listenAddress;

public class TestHelpers
{
    public static Exception executionIsExpectedToFail( Runnable runnable )
    {
        return executionIsExpectedToFail( runnable, RuntimeException.class );
    }

    public static <E extends Exception> E executionIsExpectedToFail( Runnable runnable, Class<E> exceptionClass )
    {
        try
        {
            runnable.run();
        }
        catch ( Exception e )
        {
            if ( !exceptionClass.isInstance( e ) )
            {
                throw new AssertionError( format( "Exception %s is not of type %s", e.getClass().getName(), exceptionClass.getName() ), e );
            }
            return (E) e;
        }
        throw new AssertionError( "The code expected to fail hasn't failed" );
    }

    public static int runBackupToolFromOtherJvmToGetExitCode( File neo4jHome, String... args ) throws Exception
    {
        return runBackupToolFromOtherJvmToGetExitCode( neo4jHome, System.out, System.err, true, args );
    }

    public static int runBackupToolFromOtherJvmToGetExitCode( File neo4jHome, PrintStream outPrintStream, PrintStream errPrintStream,
            boolean debug, String... args ) throws Exception
    {
        List<String> allArgs =
                new ArrayList<>( Arrays.asList( ProcessUtil.getJavaExecutable().toString(), "-cp", ProcessUtil.getClassPath(), AdminTool.class.getName() ) );
        allArgs.add( "backup" );
        allArgs.addAll( Arrays.asList( args ) );

        ProcessBuilder processBuilder = new ProcessBuilder().command( allArgs.toArray( new String[allArgs.size()]));
        processBuilder.environment().put( "NEO4J_HOME", neo4jHome.getAbsolutePath() );
        if ( debug )
        {
            processBuilder.environment().put( "NEO4J_DEBUG", "anything_works" );
        }
        Process process = processBuilder.start();
        ProcessStreamHandler processStreamHandler =
                new ProcessStreamHandler( process, false, "", StreamConsumer.IGNORE_FAILURES, outPrintStream, errPrintStream );
        return processStreamHandler.waitForResult();
    }

    public static String backupAddressCc( GraphDatabaseAPI graphDatabase )
    {
        ListenSocketAddress hostnamePort = graphDatabase.getDependencyResolver().resolveDependency( Config.class ).get(
                listenAddress( "dbms.backup.address", 6000 ) );

        return hostnamePort.toString();
    }

    public static String backupAddressHa( GraphDatabaseAPI graphDatabase )
    {
        HostnamePort hostnamePort = graphDatabase
                .getDependencyResolver()
                .resolveDependency( Config.class )
                .get( OnlineBackupSettings.online_backup_server );

        return hostnamePort.toString();
    }
}


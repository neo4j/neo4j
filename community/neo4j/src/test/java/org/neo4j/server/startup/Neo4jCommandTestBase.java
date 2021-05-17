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
package org.neo4j.server.startup;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import picocli.CommandLine;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import org.neo4j.configuration.BootloaderSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.util.Preconditions;

import static org.apache.commons.lang3.SystemUtils.IS_OS_WINDOWS;

abstract class Neo4jCommandTestBase extends BootloaderCommandTestBase
{
    Path pidFile;

    @Override
    @BeforeEach
    void setUp() throws Exception
    {
        super.setUp();
        pidFile = config.get( BootloaderSettings.pid_file );
        if ( IS_OS_WINDOWS )
        {
            URL resource = getClass().getClassLoader().getResource( WindowsBootloaderOs.PRUNSRV_AMD_64_EXE );
            Preconditions.checkState( resource != null, "Couldn't find windows binaries for running boot loader" );
            Path toolsInResources;
            if ( resource.getProtocol().equals( "jar" ) )
            {
                // We're running in a scenario where we're probably not in an IDE and we have the prunsrv binaries in a jar file,
                // we have to unpack it from that jar and place it in our own equivalent location
                toolsInResources = Path.of( getClass().getClassLoader().getResource( "" ).toURI() ).toAbsolutePath();
                copyDependentTestResourceFromJar( toolsInResources, WindowsBootloaderOs.PRUNSRV_AMD_64_EXE );
                copyDependentTestResourceFromJar( toolsInResources, WindowsBootloaderOs.PRUNSRV_I_386_EXE );
            }
            else
            {
                // We're running in an IDE or we're running the component which has the prunsrv binaries test resources directly
                toolsInResources = Path.of( resource.toURI() ).toAbsolutePath().getParent();
            }
            FileUtils.copyDirectory( toolsInResources, config.get( BootloaderSettings.windows_tools_directory ) );
        }
    }

    private void copyDependentTestResourceFromJar( Path toolsPath, String fileName ) throws IOException
    {
        try ( InputStream in = getClass().getClassLoader().getResourceAsStream( fileName );
                OutputStream out = new FileOutputStream( toolsPath.resolve( fileName ).toFile() ) )
        {
            IOUtils.copy( in, out );
        }
    }

    @AfterEach
    void tearDown()
    {
        super.tearDown();
        Optional<ProcessHandle> handle = getProcess();
        if ( handle.isPresent() && handle.get().isAlive() )
        {
            handle.get().destroy();
        }
    }

    protected Optional<ProcessHandle> getProcess()
    {
        Long pid = pidFromFile();
        if ( pid != null )
        {
            return ProcessHandle.of( pid );
        }
        return Optional.empty();
    }

    protected Long pidFromFile()
    {
        String pidStr = readFile( pidFile );
        return StringUtils.isNotEmpty( pidStr ) ? Long.parseLong( pidStr ) : null;
    }

    protected String getDebugLogLines()
    {
        return readFile( config.get( GraphDatabaseSettings.store_internal_log_path ) );
    }

    protected String getUserLogLines()
    {
        return readFile( config.get( GraphDatabaseSettings.store_user_log_path ) );
    }

    private static String readFile( Path file )
    {
        try
        {
            return Files.readString( file );
        }
        catch ( IOException e )
        {
            return "";
        }
    }

    @Override
    protected int execute( List<String> args, Map<String, String> env )
    {
        HashMap<String,String> environment = new HashMap<>( env );
        environment.putIfAbsent( Bootloader.ENV_NEO4J_START_WAIT, "0" );
        return super.execute( args, environment );
    }

    @Override
    protected CommandLine createCommand( PrintStream out, PrintStream err, Function<String,String> envLookup, Function<String,String> propLookup )
    {
        Neo4jCommand.Neo4jBootloaderContext ctx = new Neo4jCommand.Neo4jBootloaderContext( out, err, envLookup, propLookup, entrypoint() );
        return Neo4jCommand.asCommandLine( ctx );
    }

    protected abstract Class<? extends EntryPoint> entrypoint();

    public static boolean isCurrentlyRunningAsWindowsAdmin()
    {
        // The problem: windows-tests in this class want to e.g. install and start the neo4j service to test it on the highest level.
        // Now, if you're running this test as a non-admin the service util which Neo4j uses to accomplish this (prunsrv-<arch>.exe)
        // will display a GUI prompt to elevate the user (if the user is an administrator) to administrator privileges. As an automated
        // test this doesn't work because there must be a user sitting ready, clicking that button.
        // The solution: the solution is NOT to simply check whether or not the user is an administrator because users can typically
        // be administrators, but runs most things w/o administrator privileges. So instead use something which figures out which
        // privileges the user runs with right now.

        try
        {
            // "net session" will exit with code 0 if you're currently running as administrator, otherwise code 2
            return Runtime.getRuntime().exec( new String[]{"cmd.exe", "/C", "net session"} ).waitFor() == 0;
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }
}

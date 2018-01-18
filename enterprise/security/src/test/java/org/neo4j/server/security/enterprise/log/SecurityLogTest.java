/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.server.security.enterprise.log;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.time.ZoneOffset;
import java.util.Scanner;
import java.util.TimeZone;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.Level;
import org.neo4j.logging.LogTimeZone;
import org.neo4j.server.security.enterprise.configuration.SecuritySettings;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static java.lang.String.format;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.array;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class SecurityLogTest
{
    @Rule
    public EphemeralFileSystemRule fileSystemRule = new EphemeralFileSystemRule();

    private Config config = Config.defaults(
            stringMap( SecuritySettings.store_security_log_rotation_threshold.name(), "5",
                    SecuritySettings.store_security_log_rotation_delay.name(), "1ms" ) );

    @Test
    public void shouldRotateLog() throws IOException
    {
        SecurityLog securityLog = new SecurityLog( config, fileSystemRule.get(), Runnable::run );
        securityLog.info( "line 1" );
        securityLog.info( "line 2" );

        FileSystemAbstraction fs = fileSystemRule.get();

        File activeLogFile = config.get( SecuritySettings.security_log_filename );
        assertThat( fs.fileExists( activeLogFile ), equalTo( true ) );
        assertThat( fs.fileExists( archive( 1 ) ), equalTo( true ) );
        assertThat( fs.fileExists( archive( 2 ) ), equalTo( false ) );

        String[] activeLines = readLogFile( fs, activeLogFile );
        assertThat( activeLines, array( containsString( "line 2" ) ) );

        String[] archiveLines = readLogFile( fs, archive( 1 ) );
        assertThat( archiveLines, array( containsString( "line 1" ) ) );
    }

    @Test
    public void logUseSystemTimeZoneIfConfigured() throws Exception
    {
        TimeZone defaultTimeZone = TimeZone.getDefault();
        try
        {
            checkLogTimeZone( 4, "+0400" );
            checkLogTimeZone( -8, "-0800" );
        }
        finally
        {
            TimeZone.setDefault( defaultTimeZone );
        }
    }

    private void checkLogTimeZone( int hoursShift, String timeZoneSuffix ) throws Exception
    {
        TimeZone.setDefault( TimeZone.getTimeZone( ZoneOffset.ofHours( hoursShift ) ) );
        Config timeZoneConfig = Config.defaults( GraphDatabaseSettings.db_timezone, LogTimeZone.SYSTEM.name() );
        SecurityLog securityLog = new SecurityLog( timeZoneConfig, fileSystemRule.get(), Runnable::run );
        securityLog.info( "line 1" );

        FileSystemAbstraction fs = fileSystemRule.get();
        File activeLogFile = timeZoneConfig.get( SecuritySettings.security_log_filename );
        String[] activeLines = readLogFile( fs, activeLogFile );
        assertThat( activeLines, array( containsString( timeZoneSuffix ) ) );
        fileSystemRule.clear();
    }

    @Test
    public void shouldHonorLogLevel() throws Throwable
    {
        writeAllLevelsAndShutdown( withLogLevel( Level.DEBUG ), "debug" );
        writeAllLevelsAndShutdown( withLogLevel( Level.INFO ), "info" );
        writeAllLevelsAndShutdown( withLogLevel( Level.WARN ), "warn" );
        writeAllLevelsAndShutdown( withLogLevel( Level.ERROR ), "error" );

        FileSystemAbstraction fs = fileSystemRule.get();
        File activeLogFile = config.get( SecuritySettings.security_log_filename );
        String[] activeLines = readLogFile( fs, activeLogFile );
        assertThat( activeLines, array(
                containsString( "debug: debug line" ),
                containsString( "debug: info line" ),
                containsString( "debug: warn line" ),
                containsString( "debug: error line" ),

                containsString( "info: info line" ),
                containsString( "info: warn line" ),
                containsString( "info: error line" ),

                containsString( "warn: warn line" ),
                containsString( "warn: error line" ),

                containsString( "error: error line" )
            ) );
    }

    private void writeAllLevelsAndShutdown( SecurityLog securityLog, String tag ) throws Throwable
    {
        securityLog.debug( format( "%s: debug line", tag ) );
        securityLog.info( format( "%s: info line", tag ) );
        securityLog.warn( format( "%s: warn line", tag ) );
        securityLog.error( format( "%s: error line", tag ) );
        securityLog.shutdown();
    }

    private SecurityLog withLogLevel( Level debug ) throws IOException
    {
        return new SecurityLog(
                Config.defaults( SecuritySettings.security_log_level, debug.name() ),
                fileSystemRule.get(),
                Runnable::run
            );
    }

    private String[] readLogFile( FileSystemAbstraction fs, File activeLogFile ) throws IOException
    {
        Scanner scan = new Scanner( fs.openAsInputStream( activeLogFile ) );
        scan.useDelimiter( "\\Z" );
        String allLines = scan.next();
        scan.close();
        return allLines.split( "\\n" );
    }

    private File archive( int archiveNumber )
    {
        return new File( format( "%s.%d", config.get( SecuritySettings.security_log_filename ), archiveNumber ) );
    }
}

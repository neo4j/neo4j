/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.enterprise;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Scanner;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.enterprise.configuration.EnterpriseEditionSettings;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class SecurityLogTest
{
    @Rule
    public EphemeralFileSystemRule fileSystemRule = new EphemeralFileSystemRule();

    Config config = Config.defaults().augment(
            stringMap( EnterpriseEditionSettings.store_security_log_rotation_threshold.name(), "5",
                    EnterpriseEditionSettings.store_security_log_rotation_delay.name(), "1ms" ) );

    @Test
    public void shouldRotateLog() throws IOException
    {
        SecurityLog securityLog = new SecurityLog( config, fileSystemRule.get(), Runnable::run );
        securityLog.info( "line 1" );
        securityLog.info( "line 2" );

        FileSystemAbstraction fs = fileSystemRule.get();

        File activeLogFile = config.get( EnterpriseEditionSettings.security_log_filename );
        assertThat( fs.fileExists( activeLogFile ), equalTo( true ) );
        assertThat( fs.fileExists( archive( 1 ) ), equalTo( true ) );
        assertThat( fs.fileExists( archive( 2 ) ), equalTo( false ) );

        String[] activeLines = readLogFile( fs, activeLogFile );
        assertThat( activeLines.length, equalTo( 1 ) );
        assertThat( activeLines[0], containsString( "line 2" ) );

        String[] archiveLines = readLogFile( fs, archive( 1 ) );
        assertThat( archiveLines.length, equalTo( 1 ) );
        assertThat( archiveLines[0], containsString( "line 1" ) );
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
        return new File( String.format( "%s.%d", config.get( EnterpriseEditionSettings.security_log_filename ),
                archiveNumber ) );
    }
}

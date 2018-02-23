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
package org.neo4j.kernel.info;

import org.junit.jupiter.api.Test;

import org.neo4j.logging.BufferingLog;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.kernel.info.JvmChecker.INCOMPATIBLE_JVM_VERSION_WARNING;
import static org.neo4j.kernel.info.JvmChecker.INCOMPATIBLE_JVM_WARNING;

public class JVMCheckerTest
{
    @Test
    public void shouldIssueWarningWhenUsingHotspotServerVmVersion7()
    {
        BufferingLog bufferingLogger = new BufferingLog();

        new JvmChecker( bufferingLogger, new CannedJvmMetadataRepository( "Java HotSpot(TM) 64-Bit Server VM",
                "1.7.0-b147" ) ).checkJvmCompatibilityAndIssueWarning();

        assertThat( bufferingLogger.toString().trim(), is( INCOMPATIBLE_JVM_VERSION_WARNING ) );
    }

    @Test
    public void shouldNotIssueWarningWhenUsingHotspotServerVmVersion8()
    {
        BufferingLog bufferingLogger = new BufferingLog();

        new JvmChecker( bufferingLogger, new CannedJvmMetadataRepository( "Java HotSpot(TM) 64-Bit Server VM",
                "1.8.0_45" ) ).checkJvmCompatibilityAndIssueWarning();

        assertTrue( bufferingLogger.toString().isEmpty() );
    }

    @Test
    public void shouldNotIssueWarningWhenUsingIbmJ9Vm()
    {
        BufferingLog bufferingLogger = new BufferingLog();

        new JvmChecker( bufferingLogger, new CannedJvmMetadataRepository( "IBM J9 VM", "1.8" ) )
                .checkJvmCompatibilityAndIssueWarning();

        assertTrue( bufferingLogger.toString().isEmpty() );
    }

    @Test
    public void shouldIssueWarningWhenUsingHotspotServerVmVersion7InThe32BitVersion()
    {
        BufferingLog bufferingLogger = new BufferingLog();

        new JvmChecker( bufferingLogger, new CannedJvmMetadataRepository( "Java HotSpot(TM) Server VM",
                "1.7.0_25-b15" ) ).checkJvmCompatibilityAndIssueWarning();

        assertThat( bufferingLogger.toString().trim(), is( INCOMPATIBLE_JVM_VERSION_WARNING ) );
    }

    @Test
    public void shouldIssueWarningWhenUsingOpenJDKServerVmVersion7()
    {
        BufferingLog bufferingLogger = new BufferingLog();

        new JvmChecker( bufferingLogger, new CannedJvmMetadataRepository( "OpenJDK 64-Bit Server VM",
                "1.7.0-b147" ) ).checkJvmCompatibilityAndIssueWarning();

        assertThat( bufferingLogger.toString().trim(), is( INCOMPATIBLE_JVM_VERSION_WARNING ) );
    }

    @Test
    public void shouldIssueWarningWhenUsingOpenJDKClientVmVersion7()
    {
        BufferingLog bufferingLogger = new BufferingLog();

        new JvmChecker( bufferingLogger, new CannedJvmMetadataRepository( "OpenJDK Client VM",
                "1.7.0-b147" ) ).checkJvmCompatibilityAndIssueWarning();

        assertThat( bufferingLogger.toString().trim(), is( INCOMPATIBLE_JVM_VERSION_WARNING ) );
    }

    @Test
    public void shouldIssueWarningWhenUsingUnsupportedJvm()
    {
        BufferingLog bufferingLogger = new BufferingLog();

        new JvmChecker( bufferingLogger, new CannedJvmMetadataRepository( "MyOwnJDK 64-Bit Awesome VM",
                "1.7" ) ).checkJvmCompatibilityAndIssueWarning();

        assertThat( bufferingLogger.toString().trim(), is( INCOMPATIBLE_JVM_WARNING ) );
    }

    @Test
    public void shouldIssueWarningWhenUsingUnsupportedJvmVersion()
    {
        BufferingLog bufferingLogger = new BufferingLog();

        new JvmChecker( bufferingLogger, new CannedJvmMetadataRepository( "Java HotSpot(TM) 64-Bit Server VM",
                "1.6.42_87" ) ).checkJvmCompatibilityAndIssueWarning();

        assertThat( bufferingLogger.toString().trim(), is( INCOMPATIBLE_JVM_VERSION_WARNING ) );
    }
}

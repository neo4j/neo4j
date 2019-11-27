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
package org.neo4j.kernel.info;

import org.junit.jupiter.api.Test;

import org.neo4j.configuration.ExternalSettings;
import org.neo4j.logging.BufferingLog;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.kernel.info.JvmChecker.INCOMPATIBLE_JVM_VERSION_WARNING;
import static org.neo4j.kernel.info.JvmChecker.memorySettingWarning;

class JVMCheckerTest
{
    @Test
    void shouldIssueWarningWhenUsingHotspotServerVmVersion12()
    {
        BufferingLog bufferingLogger = new BufferingLog();

        new JvmChecker( bufferingLogger, new CannedJvmMetadataRepository( "Java HotSpot(TM) 64-Bit Server VM",
                "12" ) ).checkJvmCompatibilityAndIssueWarning();

        assertThat( bufferingLogger.toString() ).contains( INCOMPATIBLE_JVM_VERSION_WARNING );
    }

    @Test
    void shouldNotIssueWarningWhenUsingHotspotServerVmVersion11()
    {
        BufferingLog bufferingLogger = new BufferingLog();

        new JvmChecker( bufferingLogger, new CannedJvmMetadataRepository( "Java HotSpot(TM) 64-Bit Server VM",
                "11" ) ).checkJvmCompatibilityAndIssueWarning();

        assertThat( bufferingLogger.toString() ).doesNotContain( INCOMPATIBLE_JVM_VERSION_WARNING );
    }

    @Test
    void shouldIssueWarningWhenUsingUnsupportedJvmVersion()
    {
        BufferingLog bufferingLogger = new BufferingLog();

        new JvmChecker( bufferingLogger, new CannedJvmMetadataRepository( "Java HotSpot(TM) 64-Bit Server VM",
                "22.33.44.55" ) ).checkJvmCompatibilityAndIssueWarning();

        assertThat( bufferingLogger.toString() ).contains( INCOMPATIBLE_JVM_VERSION_WARNING );
    }

    @Test
    void warnAboutMissingInitialHeapSize()
    {
        BufferingLog bufferingLogger = new BufferingLog();

        new JvmChecker( bufferingLogger, new CannedJvmMetadataRepository( "Java HotSpot(TM) 64-Bit Server VM",
                "11.0.2+9", singletonList( "-XMx" ), 12, 23 ) ).checkJvmCompatibilityAndIssueWarning();

        assertThat( bufferingLogger.toString() ).contains( memorySettingWarning( ExternalSettings.initialHeapSize, 12 ) );
    }

    @Test
    void warnAboutMissingMaximumHeapSize()
    {
        BufferingLog bufferingLogger = new BufferingLog();

        new JvmChecker( bufferingLogger, new CannedJvmMetadataRepository( "Java HotSpot(TM) 64-Bit Server VM",
                "11", singletonList( "-XMs" ), 12, 23 ) ).checkJvmCompatibilityAndIssueWarning();

        assertThat( bufferingLogger.toString() ).contains( memorySettingWarning( ExternalSettings.maxHeapSize, 23 ) );
    }

    @Test
    void warnAboutMissingHeapSizes()
    {
        BufferingLog bufferingLogger = new BufferingLog();

        new JvmChecker( bufferingLogger, new CannedJvmMetadataRepository( "Java HotSpot(TM) 64-Bit Server VM",
                "11.0.1" ) ).checkJvmCompatibilityAndIssueWarning();

        assertThat( bufferingLogger.toString() ).contains( memorySettingWarning( ExternalSettings.initialHeapSize, 1 ) );
        assertThat( bufferingLogger.toString() ).contains( memorySettingWarning( ExternalSettings.maxHeapSize, 2 ) );
    }

    @Test
    void doNotWarnAboutMissingHeapSizesWhenOptionsSpecified()
    {
        BufferingLog bufferingLogger = new BufferingLog();

        new JvmChecker( bufferingLogger, new CannedJvmMetadataRepository( "Java HotSpot(TM) 64-Bit Server VM", "11.0.2",
                asList( "-xMx", "-xmS" ), 1, 2 ) ).checkJvmCompatibilityAndIssueWarning();

        assertThat( bufferingLogger.toString() ).doesNotContain( memorySettingWarning( ExternalSettings.initialHeapSize, 1 ) );
        assertThat( bufferingLogger.toString() ).doesNotContain( memorySettingWarning( ExternalSettings.maxHeapSize, 2 ) );
    }
}

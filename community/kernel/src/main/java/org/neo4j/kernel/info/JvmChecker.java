/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import java.lang.management.MemoryUsage;
import java.util.List;
import java.util.regex.Pattern;

import org.neo4j.configuration.ExternalSettings;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.logging.Log;

public class JvmChecker
{
    static final String INCOMPATIBLE_JVM_WARNING = "You are using an unsupported Java runtime. Please" +
            " use Oracle(R) Java(TM) Runtime Environment 8, OpenJDK(TM) 8 or IBM J9.";
    static final String INCOMPATIBLE_JVM_VERSION_WARNING = "You are using an unsupported version of " +
            "the Java runtime. Please use Oracle(R) Java(TM) Runtime Environment 8, OpenJDK(TM) 8 or IBM J9.";
    private static final Pattern SUPPORTED_JAVA_NAME_PATTERN =
            Pattern.compile( "(Java HotSpot\\(TM\\)|OpenJDK|IBM) (64-Bit Server|Server|Client|J9) VM" );
    private static final Pattern SUPPORTED_JAVA_VERSION_PATTERN = Pattern.compile( "^1\\.[8].*" );

    private final Log log;
    private final JvmMetadataRepository jvmMetadataRepository;

    public JvmChecker( Log log, JvmMetadataRepository jvmMetadataRepository )
    {
        this.log = log;
        this.jvmMetadataRepository = jvmMetadataRepository;
    }

    public void checkJvmCompatibilityAndIssueWarning()
    {
        String javaVmName = jvmMetadataRepository.getJavaVmName();
        String javaVersion = jvmMetadataRepository.getJavaVersion();

        if ( !SUPPORTED_JAVA_NAME_PATTERN.matcher( javaVmName ).matches() )
        {
            log.warn( INCOMPATIBLE_JVM_WARNING );
        }
        else if ( !SUPPORTED_JAVA_VERSION_PATTERN.matcher( javaVersion ).matches() )
        {
            log.warn( INCOMPATIBLE_JVM_VERSION_WARNING );
        }
        List<String> jvmArguments = jvmMetadataRepository.getJvmInputArguments();
        MemoryUsage heapMemoryUsage = jvmMetadataRepository.getHeapMemoryUsage();
        if ( !containsOption( jvmArguments, "-Xmx" ) )
        {
            log.warn( memorySettingWarning( ExternalSettings.maxHeapSize, heapMemoryUsage.getMax() ) );
        }
        if ( !containsOption( jvmArguments, "-Xms" ) )
        {
            log.warn( memorySettingWarning( ExternalSettings.initialHeapSize, heapMemoryUsage.getInit() ) );
        }
    }

    static String memorySettingWarning( Setting<String> setting, long currentUsage )
    {
        return "The " + setting.name() + " setting has not been configured. It is recommended that this " +
                "setting is always explicitly configured, to ensure the system has a balanced configuration. " +
                "Until then, a JVM computed heuristic of " + currentUsage + " bytes is used instead. " +
                "Run `neo4j-admin memrec` for memory configuration suggestions.";
    }

    private static boolean containsOption( List<String> jvmArguments, String option )
    {
        String normalizedOption = option.toUpperCase();
        return jvmArguments.stream().anyMatch( o -> o.toUpperCase().startsWith( normalizedOption ) );
    }
}

/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.neo4j.kernel.impl.util.StringLogger;

public class JvmChecker
{
    public static final String INCOMPATIBLE_JVM_WARNING = "You are using an unsupported Java runtime. Please" +
            " use Oracle(R) Java(TM) Runtime Environment 7.";
    public static final String INCOMPATIBLE_JVM_VERSION_WARNING = "You are using an unsupported version of " +
            "the Java runtime. Please use Oracle(R) Java(TM) Runtime Environment 7.";

    private final StringLogger stringLogger;
    private final JvmMetadataRepository jvmMetadataRepository;

    public JvmChecker( StringLogger stringLogger, JvmMetadataRepository jvmMetadataRepository )
    {
        this.stringLogger = stringLogger;
        this.jvmMetadataRepository = jvmMetadataRepository;
    }

    public void checkJvmCompatibilityAndIssueWarning()
    {
        String javaVmName = jvmMetadataRepository.getJavaVmName();
        String javaVersion = jvmMetadataRepository.getJavaVersion();

        if ( !javaVmName.matches( "Java HotSpot\\(TM\\) (64-Bit Server|Server|Client) VM" ) )
        {
            stringLogger.warn( INCOMPATIBLE_JVM_WARNING );
        }
        else if ( !javaVersion.matches( "^1\\.7.*" ) )
        {
            stringLogger.warn( INCOMPATIBLE_JVM_VERSION_WARNING );
        }
    }
}

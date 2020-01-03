/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.test.extension;

import org.junit.jupiter.api.Test;
import org.junit.platform.launcher.Launcher;
import org.junit.platform.launcher.LauncherDiscoveryRequest;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder;
import org.junit.platform.launcher.core.LauncherFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectMethod;
import static org.neo4j.test.extension.DirectoryExtensionLifecycleVerificationTest.ConfigurationParameterCondition.TEST_TOGGLE;
import static org.neo4j.test.extension.ExecutionSharedContext.CONTEXT;
import static org.neo4j.test.extension.ProfilerExtensionVerificationTest.TEST_DIR;

class ProfilerExtensionTest
{
    @Test
    void passingTestsMustNotProduceProfilerOutput()
    {
        CONTEXT.clear();
        execute( "testThatPasses" );
        File testDir = CONTEXT.getValue( TEST_DIR );
        assertFalse( testDir.exists() ); // The TestDirectory extension deletes the test directory when the test passes.
    }

    @Test
    void failingTestsMustProduceProfilerOutput() throws IOException
    {
        CONTEXT.clear();
        execute( "testThatFails" );
        File testDir = CONTEXT.getValue( TEST_DIR );
        assertTrue( testDir.exists() );
        assertTrue( testDir.isDirectory() );
        File profileData = new File( testDir, "profiler-output.txt" );
        assertTrue( profileData.exists() );
        assertTrue( profileData.isFile() );
        try ( Stream<String> lines = Files.lines( profileData.toPath() ) )
        {
            assertTrue( lines.anyMatch( line -> line.contains( "someVeryExpensiveComputation" ) ) );
        }
    }

    private static void execute( String testName, TestExecutionListener... testExecutionListeners )
    {
        LauncherDiscoveryRequest discoveryRequest = LauncherDiscoveryRequestBuilder.request()
                .selectors( selectMethod( ProfilerExtensionVerificationTest.class, testName ))
                .configurationParameter( TEST_TOGGLE, "true" )
                .build();
        Launcher launcher = LauncherFactory.create();
        launcher.execute( discoveryRequest, testExecutionListeners );
    }
}

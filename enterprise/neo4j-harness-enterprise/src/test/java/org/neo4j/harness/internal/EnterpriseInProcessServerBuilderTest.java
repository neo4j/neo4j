/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.harness.internal;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;

import org.neo4j.harness.EnterpriseTestServerBuilders;
import org.neo4j.harness.ServerControls;
import org.neo4j.test.SuppressOutput;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.server.HTTP;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class EnterpriseInProcessServerBuilderTest
{
    @Rule
    public TargetDirectory.TestDirectory testDir = TargetDirectory.testDirForTest( EnterpriseInProcessServerBuilderTest.class );

    @Rule public SuppressOutput suppressOutput = SuppressOutput.suppressAll();

    @Test
    public void shouldLaunchAServerInSpecifiedDirectory() throws Exception
    {
        // Given
        File workDir = new File(testDir.directory(), "specific");
        workDir.mkdir();

        // When
        try(ServerControls server = EnterpriseTestServerBuilders.newInProcessBuilder( workDir ).newServer())
        {
            // Then
            assertThat( HTTP.GET( server.httpURI().toString() ).status(), equalTo( 200 ) );
            assertThat( workDir.list().length, equalTo(1));
        }

        // And after it's been closed, it should've cleaned up after itself.
        assertThat( Arrays.toString( workDir.list() ), workDir.list().length, equalTo( 0 ) );
    }
}

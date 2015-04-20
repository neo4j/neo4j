/*
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
package org.neo4j.server;

import java.io.File;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.test.Mute;
import org.neo4j.test.server.ExclusiveServerTestBase;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

public abstract class BaseBootstrapperTest extends ExclusiveServerTestBase
{
    @Rule
    public final Mute mute = Mute.muteAll();

    private Bootstrapper bootstrapper;

    @Before
    @After
    public void cleanUpAfterBootstrapper() throws Exception
    {
        if ( bootstrapper != null )
        {
            String baseDir = bootstrapper.getServer().getDatabase().getLocation();
            System.out.println( baseDir );
            bootstrapper.stop();
            FileUtils.deleteRecursively( new File( baseDir ) );
        }
    }

    protected abstract Class<? extends Bootstrapper> bootstrapperClass();

    protected abstract Bootstrapper newBootstrapper();

    @Test
    public void shouldLoadAppropriateBootstrapper()
    {
        assertThat( Bootstrapper.loadMostDerivedBootstrapper(), is( instanceOf( bootstrapperClass() ) ) );
    }

    @Test
    public void shouldStartStopNeoServerWithoutAnyConfigFiles()
    {
        // Given
        bootstrapper = newBootstrapper();

        // When
        Integer resultCode = bootstrapper.start();

        // Then
        assertEquals( Bootstrapper.OK, resultCode );
        assertNotNull( bootstrapper.getServer() );

        bootstrapper.stop();
    }
}

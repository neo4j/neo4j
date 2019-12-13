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
package org.neo4j.server;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class BlockingBootstrapperTest
{
    @Rule
    public TestDirectory homeDir = TestDirectory.testDirectory();

    @Test
    public void shouldBlockUntilStoppedIfTheWrappedStartIsSuccessful() throws Throwable
    {
        AtomicInteger status = new AtomicInteger();
        AtomicBoolean exited = new AtomicBoolean( false );
        AtomicBoolean running = new AtomicBoolean( false );

        BlockingBootstrapper bootstrapper = new BlockingBootstrapper( new Bootstrapper()
        {
            @Override
            public int start( File homeDir, Optional<File> configFile, Map<String, String> configOverrides )
            {
                running.set( true );
                return 0;
            }

            @Override
            public int stop()
            {
                running.set( false );
                return 0;
            }
        } );

        new Thread( () ->
        {
            status.set( bootstrapper.start( homeDir.directory( "home-dir" ), Optional.empty(), Collections.emptyMap() ) );
            exited.set( true );
        } ).start();

        assertEventually( "Wrapped was not started", running::get, is( true ), 10, TimeUnit.SECONDS );
        assertThat( "Bootstrapper exited early", exited.get(), is( false ) );

        bootstrapper.stop();

        assertEventually( "Wrapped was not stopped", running::get, is( false ), 10, TimeUnit.SECONDS );
        assertEventually( "Bootstrapper did not exit", exited::get, is( true ), 10, TimeUnit.SECONDS );
        assertThat( "Bootstrapper did not propagate exit status", status.get(), is( 0 ) );
    }

    @Test
    public void shouldNotBlockIfTheWrappedStartIsUnsuccessful() throws Throwable
    {
        AtomicInteger status = new AtomicInteger();
        AtomicBoolean exited = new AtomicBoolean( false );

        BlockingBootstrapper bootstrapper = new BlockingBootstrapper( new Bootstrapper()
        {
            @Override
            public int start( File homeDir, Optional<File> configFile, Map<String, String> configOverrides )
            {
                return 1;
            }

            @Override
            public int stop()
            {
                return 0;
            }
        } );

        new Thread( () ->
        {
            status.set( bootstrapper.start( homeDir.directory( "home-dir" ), Optional.empty(), Collections.emptyMap() ) );
            exited.set( true );
        } ).start();

        assertEventually( "Blocked unexpectedly", exited::get, is( true ), 10, TimeUnit.SECONDS );
        assertThat( "Bootstrapper did not propagate exit status", status.get(), is( 1 ) );
    }
}

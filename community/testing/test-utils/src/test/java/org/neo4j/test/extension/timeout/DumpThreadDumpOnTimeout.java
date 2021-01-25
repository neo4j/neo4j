/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.test.extension.timeout;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.opentest4j.AssertionFailedError;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.lang.Thread.sleep;
import static java.time.Duration.ofMillis;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class DumpThreadDumpOnTimeout
{
    @Test
    void dumpOnTimeoutPreemptively()
    {
        assertTimeoutPreemptively( ofMillis( 10 ), () ->
        {
            sleep( TimeUnit.MINUTES.toMillis( 1 ) );
        } );
    }

    @Test
    @Timeout( value = 10, unit = TimeUnit.MILLISECONDS )
    void dumpOnTimeoutAnnotation() throws InterruptedException
    {
        sleep( TimeUnit.MINUTES.toMillis( 1 ) );
    }

    @Test
    void dumpOnTimeoutException() throws TimeoutException
    {
        throw new TimeoutException();
    }

    @Test
    void dumpOnAssertionFailedErrorWithMessage()
    {
        throw new AssertionFailedError( "foo() timed out after 20 minutes" ); //mimic what is thrown by the default timeout
    }

    @Test
    void dumpOnCauseTimeout()
    {
        throw new RuntimeException( new TimeoutException() );
    }

    @Test
    void dumpOnSuppressedTimeout()
    {
        RuntimeException exception = new RuntimeException();
        exception.addSuppressed( new TimeoutException() );
        throw exception;
    }

    @Test
    void dumpOnDeepCauseTimeout()
    {
        RuntimeException exception = new RuntimeException( new TimeoutException() );
        for ( int i = 0; i < 10; i++ )
        {
            exception = new RuntimeException( exception );
        }
        throw exception;
    }

    @Test
    void dumpOnDeepSuppressedTimeout()
    {
        RuntimeException exception = new RuntimeException();
        exception.addSuppressed( new TimeoutException() );
        for ( int i = 0; i < 10; i++ )
        {
            exception = new RuntimeException( exception );
        }
        throw exception;
    }

    @Test
    void doNotDumpOnAssume()
    {
        assumeTrue( false );
    }

    @Test
    void doNotDumpOnAssert()
    {
        assertThat( "foo" ).isEqualTo( "bar" );
    }

    @Test
    void doNotDumpOnException()
    {
        throw new RuntimeException( "foo" );
    }

    @Test
    void doNotDumpOnDeepException()
    {
        RuntimeException exception = new RuntimeException();
        for ( int i = 0; i < 10; i++ )
        {
            exception = new RuntimeException( exception );
        }
        throw exception;
    }
}

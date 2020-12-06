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
package org.neo4j.shell.cli;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CliArgsTest
{
    private CliArgs cliArgs;

    @Before
    public void setup()
    {
        cliArgs = new CliArgs();
    }

    @Test
    public void setHost() throws Exception
    {
        cliArgs.setHost( "foo", "bar" );
        assertEquals( "foo", cliArgs.getHost() );

        cliArgs.setHost( null, "bar" );
        assertEquals( "bar", cliArgs.getHost() );
    }

    @Test
    public void setPort() throws Exception
    {
        cliArgs.setPort( 999 );
        assertEquals( 999, cliArgs.getPort() );
    }

    @Test
    public void setUsername() throws Exception
    {
        cliArgs.setUsername( "foo", "bar" );
        assertEquals( "foo", cliArgs.getUsername() );

        cliArgs.setUsername( null, "bar" );
        assertEquals( "bar", cliArgs.getUsername() );
    }

    @Test
    public void setPassword() throws Exception
    {
        cliArgs.setPassword( "foo", "bar" );
        assertEquals( "foo", cliArgs.getPassword() );

        cliArgs.setPassword( null, "bar" );
        assertEquals( "bar", cliArgs.getPassword() );
    }

    @Test
    public void setFailBehavior() throws Exception
    {
        // default
        assertEquals( FailBehavior.FAIL_FAST, cliArgs.getFailBehavior() );

        cliArgs.setFailBehavior( FailBehavior.FAIL_AT_END );
        assertEquals( FailBehavior.FAIL_AT_END, cliArgs.getFailBehavior() );
    }

    @Test
    public void getNumSampleRows() throws Exception
    {
        assertEquals( 1000, CliArgs.DEFAULT_NUM_SAMPLE_ROWS );
        assertEquals( CliArgs.DEFAULT_NUM_SAMPLE_ROWS, cliArgs.getNumSampleRows() );

        cliArgs.setNumSampleRows( null );
        assertEquals( CliArgs.DEFAULT_NUM_SAMPLE_ROWS, cliArgs.getNumSampleRows() );

        cliArgs.setNumSampleRows( 0 );
        assertEquals( CliArgs.DEFAULT_NUM_SAMPLE_ROWS, cliArgs.getNumSampleRows() );

        cliArgs.setNumSampleRows( 120 );
        assertEquals( 120, cliArgs.getNumSampleRows() );
    }

    @Test
    public void setFormat() throws Exception
    {
        // default
        assertEquals( Format.AUTO, cliArgs.getFormat() );

        cliArgs.setFormat( Format.PLAIN );
        assertEquals( Format.PLAIN, cliArgs.getFormat() );

        cliArgs.setFormat( Format.VERBOSE );
        assertEquals( Format.VERBOSE, cliArgs.getFormat() );
    }

    @Test
    public void setCypher() throws Exception
    {
        // default
        assertFalse( cliArgs.getCypher().isPresent() );

        cliArgs.setCypher( "foo" );
        assertTrue( cliArgs.getCypher().isPresent() );
        //noinspection OptionalGetWithoutIsPresent
        assertEquals( "foo", cliArgs.getCypher().get() );

        cliArgs.setCypher( null );
        assertFalse( cliArgs.getCypher().isPresent() );
    }
}

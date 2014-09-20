/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.command;

import org.junit.Test;

import org.neo4j.kernel.impl.transaction.command.CommandReader;
import org.neo4j.kernel.impl.transaction.command.CommandReaderFactory;
import org.neo4j.kernel.impl.transaction.command.PhysicalLogNeoCommandReaderV0_19;
import org.neo4j.kernel.impl.transaction.command.PhysicalLogNeoCommandReaderV0_20;
import org.neo4j.kernel.impl.transaction.command.PhysicalLogNeoCommandReaderV1;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryVersions.LEGACY_LOG_ENTRY_VERSION;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryVersions.LOG_ENTRY_VERSION_2_1;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryVersions.LOG_ENTRY_VERSION_2_2;
import static org.neo4j.kernel.impl.transaction.log.entry.LogVersions.LOG_VERSION_1_9;
import static org.neo4j.kernel.impl.transaction.log.entry.LogVersions.LOG_VERSION_2_0;
import static org.neo4j.kernel.impl.transaction.log.entry.LogVersions.LOG_VERSION_2_1;

public class CommandReaderFactoryTest
{
    @Test
    public void testReturnsV0_19ReaderForVersion0AndLogFormat1_9() throws Exception
    {
        // GIVEN
        CommandReaderFactory factory = new CommandReaderFactory.Default();

        // WHEN
        CommandReader reader = factory.newInstance( LOG_VERSION_1_9, LEGACY_LOG_ENTRY_VERSION );

        // THEN
        assertTrue( reader instanceof PhysicalLogNeoCommandReaderV0_19 );
    }

    @Test
    public void testReturnsV0_20ReaderForVersion0AndLogFormat2_0() throws Exception
    {
        // GIVEN
        CommandReaderFactory factory = new CommandReaderFactory.Default();

        // WHEN
        CommandReader reader = factory.newInstance( LOG_VERSION_2_0, LEGACY_LOG_ENTRY_VERSION );

        // THEN
        assertTrue( reader instanceof PhysicalLogNeoCommandReaderV0_20 );
    }

    @Test
    public void testReturnsV1ReaderForVersion1AndLogFormat2_1() throws Exception
    {
        // GIVEN
        CommandReaderFactory factory = new CommandReaderFactory.Default();

        // WHEN
        CommandReader reader = factory.newInstance( LOG_VERSION_2_1, LOG_ENTRY_VERSION_2_1 );

        // THEN
        assertTrue( reader instanceof PhysicalLogNeoCommandReaderV1 );
    }

    @Test
    public void testReturnsV1ReaderForVersion2AndLogFormat2_2() throws Exception
    {
        // GIVEN
        CommandReaderFactory factory = new CommandReaderFactory.Default();

        // WHEN
        CommandReader reader = factory.newInstance( LOG_VERSION_2_1, LOG_ENTRY_VERSION_2_2 );

        // THEN
        assertTrue( reader instanceof PhysicalLogNeoCommandReaderV1 );
    }

    @Test
    public void testThrowsExceptionForNonExistingVersionFor2_1() throws Exception
    {
        // GIVEN
        CommandReaderFactory factory = new CommandReaderFactory.Default();

        // WHEN
        try
        {
            factory.newInstance( LOG_VERSION_2_1, (byte) -5 );
            fail();
        }
        catch ( IllegalArgumentException e )
        {
            // THEN
            // good
        }
    }

    @Test
    public void testThrowsExceptionForNonExistingVersionFor1_9() throws Exception
    {
        // GIVEN
        CommandReaderFactory factory = new CommandReaderFactory.Default();

        // WHEN
        try
        {
            factory.newInstance( LOG_VERSION_1_9, (byte) -5 );
            fail();
        }
        catch( IllegalArgumentException e)
        {
            // THEN
            // good
        }
    }
}

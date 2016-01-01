/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.nioneo.xa;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import java.nio.ByteBuffer;

import org.junit.Test;
import org.neo4j.kernel.impl.nioneo.xa.command.PhysicalLogNeoXaCommandReaderV0;
import org.neo4j.kernel.impl.nioneo.xa.command.PhysicalLogNeoXaCommandReaderV1;

public class XaCommandReaderFactoryTest
{
    @Test
    public void testReturnsV0ReaderForVersion0() throws Exception
    {
        // GIVEN
        XaCommandReaderFactory factory = XaCommandReaderFactory.DEFAULT;

        // WHEN
        XaCommandReader reader = factory.newInstance( (byte) 0, mock( ByteBuffer.class ) );

        // THEN
        assertTrue( reader instanceof PhysicalLogNeoXaCommandReaderV0 );
    }

    @Test
    public void testReturnsV1ReaderForVersion1() throws Exception
    {
        // GIVEN
        XaCommandReaderFactory factory = XaCommandReaderFactory.DEFAULT;

        // WHEN
        XaCommandReader reader = factory.newInstance( (byte) -1, mock( ByteBuffer.class ) );

        // THEN
        assertTrue( reader instanceof PhysicalLogNeoXaCommandReaderV1 );
    }

    @Test
    public void testThrowsExceptionForNonExistingVersion() throws Exception
    {
        // GIVEN
        XaCommandReaderFactory factory = XaCommandReaderFactory.DEFAULT;

        // WHEN
        try
        {
            factory.newInstance( (byte) -5, mock( ByteBuffer.class ) );
            fail();
        }
        catch( IllegalArgumentException e)
        {
            // THEN
            // good
        }
    }
}

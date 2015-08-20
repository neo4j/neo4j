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
package org.neo4j.packstream;

import java.io.IOException;

class NullPackInput implements PackInput
{
    @Override
    public boolean hasMoreData() throws IOException
    {
        return false;
    }

    @Override
    public byte readByte() throws IOException
    {
        return 0;
    }

    @Override
    public short readShort() throws IOException
    {
        return 0;
    }

    @Override
    public int readInt() throws IOException
    {
        return 0;
    }

    @Override
    public long readLong() throws IOException
    {
        return 0;
    }

    @Override
    public double readDouble() throws IOException
    {
        return 0;
    }

    @Override
    public PackInput readBytes( byte[] into, int offset, int toRead ) throws IOException
    {
        return null;
    }

    @Override
    public byte peekByte() throws IOException
    {
        return 0;
    }

}

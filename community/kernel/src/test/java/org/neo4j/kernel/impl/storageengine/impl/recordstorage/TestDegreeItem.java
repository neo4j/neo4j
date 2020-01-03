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
package org.neo4j.kernel.impl.storageengine.impl.recordstorage;

public class TestDegreeItem
{
    private final int type;
    private final long outgoing;
    private final long incoming;

    public TestDegreeItem( int type, long outgoing, long incoming )
    {
        this.type = type;
        this.outgoing = outgoing;
        this.incoming = incoming;
    }

    public int type()
    {
        return type;
    }

    public long outgoing()
    {
        return outgoing;
    }

    public long incoming()
    {
        return incoming;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        TestDegreeItem that = (TestDegreeItem) o;
        return type == that.type && outgoing == that.outgoing && incoming == that.incoming;
    }

    @Override
    public int hashCode()
    {
        return 31 * (31 * type + (int) (outgoing ^ (outgoing >>> 32))) + (int) (incoming ^ (incoming >>> 32));
    }

    @Override
    public String toString()
    {
        return "TestDegreeItem{" +
               "type=" + type +
               ", outgoing=" + outgoing +
               ", incoming=" + incoming +
               '}';
    }
}

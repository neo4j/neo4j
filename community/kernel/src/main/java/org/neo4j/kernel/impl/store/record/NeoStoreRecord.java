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
package org.neo4j.kernel.impl.store.record;

public class NeoStoreRecord extends PrimitiveRecord
{
    public NeoStoreRecord()
    {
        super( -1 );
        setInUse( true );
    }

    @Override
    public NeoStoreRecord initialize( boolean inUse, long nextProp )
    {
        super.initialize( inUse, nextProp );
        return this;
    }

    @Override
    public void clear()
    {
        initialize( false, -1 );
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "[" +
                "used=" + inUse() +
                ",prop=" + getNextProp() +
                "]";
    }

    @Override
    public void setIdTo( PropertyRecord property )
    {
    }

    @Override
    public NeoStoreRecord clone()
    {
        NeoStoreRecord neoStoreRecord = new NeoStoreRecord();
        neoStoreRecord.setNextProp( getNextProp() );
        return neoStoreRecord;
    }
}

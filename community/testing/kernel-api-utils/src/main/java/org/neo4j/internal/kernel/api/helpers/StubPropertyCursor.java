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
package org.neo4j.internal.kernel.api.helpers;

import java.util.Map;

import org.neo4j.internal.kernel.api.DefaultCloseListenable;
import org.neo4j.internal.kernel.api.KernelReadTracer;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.token.api.TokenConstants;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;

public class StubPropertyCursor extends DefaultCloseListenable implements PropertyCursor
{
    private int offset = -1;
    private Integer[] keys;
    private Value[] values;

    void init( Map<Integer, Value> properties )
    {
        offset = -1;
        keys = properties.keySet().toArray( new Integer[0] );
        values = properties.values().toArray( new Value[0] );
    }

    @Override
    public boolean next()
    {
        return ++offset < keys.length;
    }

    @Override
    public void close()
    {
        closeInternal();
        if ( closeListener != null )
        {
            closeListener.onClosed( this );
        }
    }

    @Override
    public void closeInternal()
    {

    }

    @Override
    public boolean isClosed()
    {
        return false;
    }

    @Override
    public int propertyKey()
    {
        return keys[offset];
    }

    @Override
    public ValueGroup propertyType()
    {
        return values[offset].valueGroup();
    }

    @Override
    public Value propertyValue()
    {
        return values[offset];
    }

    @Override
    public boolean seekProperty( int property )
    {
        if ( property == TokenConstants.NO_TOKEN  )
        {
            return false;
        }
        while ( next() )
        {
            if ( property == this.propertyKey() )
            {
                return true;
            }
        }
        return false;
    }

    @Override
    public void setTracer( KernelReadTracer tracer )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void removeTracer()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }
}

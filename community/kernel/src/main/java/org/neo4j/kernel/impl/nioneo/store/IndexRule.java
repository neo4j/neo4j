/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.nioneo.store;

import java.nio.ByteBuffer;

import org.neo4j.graphdb.Label;
import org.neo4j.helpers.UTF8;

/**
 * A {@link Label} can have zero or more index rules which will have data specified in the rules indexed.
 */
public class IndexRule extends AbstractSchemaRule
{
    private final String propertyKey;
    private final byte[] encodedPropertyKey;

    public IndexRule( long id, long label, ByteBuffer serialized )
    {
        this( id, label, readPropertyKey( serialized ) );
    }

    public IndexRule( long id, long label, String propertyKey )
    {
        super( id, label, SchemaRule.Kind.INDEX_RULE );
        this.propertyKey = propertyKey;
        this.encodedPropertyKey = UTF8.encode( propertyKey );
    }

    private static String readPropertyKey( ByteBuffer serialized )
    {
        byte[] encoded = new byte[serialized.getShort()];
        serialized.get( encoded );
        return UTF8.decode( encoded );
    }
    
    public String getPropertyKey()
    {
        return this.propertyKey;
    }

    @Override
    public int length()
    {
        return super.length() + 2 + encodedPropertyKey.length;
    }

    @Override
    public void append( ByteBuffer target )
    {
        super.append( target );
        target.putShort( (short) encodedPropertyKey.length );
        target.put( encodedPropertyKey );
    }

    @Override
    public int hashCode()
    {
        return 31 * super.hashCode() + propertyKey.hashCode();
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( this == obj )
            return true;
        if ( !super.equals( obj ) )
            return false;
        if ( getClass() != obj.getClass() )
            return false;
        IndexRule other = (IndexRule) obj;
        if ( propertyKey == null )
        {
            if ( other.propertyKey != null )
                return false;
        }
        else if ( !propertyKey.equals( other.propertyKey ) )
            return false;
        return true;
    }
}

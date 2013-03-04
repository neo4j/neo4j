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

/**
 * A {@link Label} can have zero or more index rules which will have data specified in the rules indexed.
 */
public class IndexRule extends AbstractSchemaRule
{
    private final long propertyKey;

    public IndexRule( long id, long label, ByteBuffer serialized )
    {
        this( id, label, readPropertyKey( serialized ) );
    }

    public IndexRule( long id, long label, long propertyKey )
    {
        super( id, label, SchemaRule.Kind.INDEX_RULE );
        this.propertyKey = propertyKey;
    }

    private static long readPropertyKey( ByteBuffer serialized )
    {
        // Currently only one key is supported although the data format supports multiple
        int count = serialized.getShort();
        assert count == 1;
        return serialized.getLong();
    }

    public long getPropertyKey()
    {
        return propertyKey;
    }

    @Override
    public int length()
    {
        return super.length() + 2 /*number of property keys*/ + /*propertyKey.length*/1*8 /*the property keys*/;
    }

    @Override
    public void serialize( ByteBuffer target )
    {
        super.serialize( target );
        target.putShort( (short) 1/*propertyKeys.length*/ );
        target.putLong( propertyKey );
    }

    @Override
    public int hashCode()
    {
        return 31 * super.hashCode() + (int) propertyKey;
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
        if ( propertyKey != other.propertyKey )
            return false;
        return true;
    }

    @Override
    protected String innerToString()
    {
        return ", properties=" + propertyKey;
    }
}

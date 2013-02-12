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
import java.util.Arrays;

import org.neo4j.graphdb.Label;

/**
 * A {@link Label} can have zero or more index rules which will have data specified in the rules indexed.
 */
public class IndexRule extends AbstractSchemaRule
{
    private final long[] propertyKeys;
    private final State state;

    public static enum State
    {
        POPULATING( (byte)0 ),
        ONLINE    ( (byte)1 );

        private final byte byteRepresentation;

        private State(byte byteRepresentation)
        {
            this.byteRepresentation = byteRepresentation;
        }

        public byte toByte()
        {
            return byteRepresentation;
        }

        public static State fromByte( byte value )
        {
            switch ( value )
            {
                case 0: return POPULATING;
                case 1: return ONLINE;
                default:
                    throw new IllegalArgumentException( "Invalid state value " + value );
            }
        }
    }

    public IndexRule( long id, long label, ByteBuffer serialized )
    {
        this( id, label, readState(serialized), readPropertyKeys( serialized ) );
    }

    public IndexRule( long id, long label, State state, long[] propertyKeys )
    {
        super( id, label, SchemaRule.Kind.INDEX_RULE );
        this.propertyKeys = propertyKeys;
        this.state = state;
    }

    private static long[] readPropertyKeys( ByteBuffer serialized )
    {
        long[] result = new long[serialized.getShort()];
        for ( int i = 0; i < result.length; i++ )
            result[i] = serialized.getLong();
        return result;
    }

    private static State readState( ByteBuffer serialized )
    {
        return State.fromByte( serialized.get() );
    }

    public long[] getPropertyKeys()
    {
        return propertyKeys;
    }

    public State getState()
    {
        return state;
    }

    @Override
    public int length()
    {
        return super.length() + 1 /* state metadata */ + 2 /*number of property keys*/ + propertyKeys.length*8 /*the property keys*/;
    }

    @Override
    public void serialize( ByteBuffer target )
    {
        super.serialize( target );
        target.put( state.toByte() );
        target.putShort( (short) propertyKeys.length );
        for ( long key : propertyKeys )
            target.putLong( key );
    }

    @Override
    public int hashCode()
    {
        return 31 * super.hashCode() + Arrays.hashCode( propertyKeys );
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
        if ( !Arrays.equals( propertyKeys, other.propertyKeys ) )
            return false;
        return true;
    }

    @Override
    protected String innerToString()
    {
        return ", properties=" + Arrays.toString( propertyKeys );
    }
}

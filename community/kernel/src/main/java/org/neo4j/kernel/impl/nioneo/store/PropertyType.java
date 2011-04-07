/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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

/**
 * Defines valid property types.
 */
public enum PropertyType
{
    ILLEGAL( 0 )
    {
        @Override
        public Object getValue( PropertyRecord record, PropertyStore store )
        {
            throw new InvalidRecordException( "Invalid type: 0 for record " + record );
        }
    },
    INT( 1 )
    {
        @Override
        public Object getValue( PropertyRecord record, PropertyStore store )
        {
            return Integer.valueOf( (int) record.getPropBlock() );
        }
    },
    STRING( 2 )
    {
        @Override
        public Object getValue( PropertyRecord record, PropertyStore store )
        {
            if ( store == null ) return null;
            return store.getStringFor( record );
        }
    },
    BOOL( 3 )
    {
        @Override
        public Object getValue( PropertyRecord record, PropertyStore store )
        {
            if ( record.getPropBlock() == 1 ) return Boolean.TRUE;
            return Boolean.FALSE;
        }
    },
    DOUBLE( 4 )
    {
        @Override
        public Object getValue( PropertyRecord record, PropertyStore store )
        {
            return Double.valueOf( Double.longBitsToDouble( record.getPropBlock() ) );
        }
    },
    FLOAT( 5 )
    {
        @Override
        public Object getValue( PropertyRecord record, PropertyStore store )
        {
            return new Float( Float.intBitsToFloat( (int) record.getPropBlock() ) );
        }
    },
    LONG( 6 )
    {
        @Override
        public Object getValue( PropertyRecord record, PropertyStore store )
        {
            return Long.valueOf( record.getPropBlock() );
        }
    },
    BYTE( 7 )
    {
        @Override
        public Object getValue( PropertyRecord record, PropertyStore store )
        {
            return Byte.valueOf( (byte) record.getPropBlock() );
        }
    },
    CHAR( 8 )
    {
        @Override
        public Object getValue( PropertyRecord record, PropertyStore store )
        {
            return Character.valueOf( (char) record.getPropBlock() );
        }
    },
    ARRAY( 9 )
    {
        @Override
        public Object getValue( PropertyRecord record, PropertyStore store )
        {
            if ( store == null ) return null;
            return store.getArrayFor( record );
        }
    },
    SHORT( 10 )
    {
        @Override
        public Object getValue( PropertyRecord record, PropertyStore store )
        {
            return Short.valueOf( (short) record.getPropBlock() );
        }
    },
    SHORT_STRING( 11 )
    {
        @Override
        public Object getValue( PropertyRecord record, PropertyStore store )
        {
            return ShortString.decode( record.getPropBlock() );
        }
    }
    ;

    private int type;

    PropertyType( int type )
    {
        this.type = type;
    }

    /**
     * Returns an int value representing the type.
     *
     * @return The int value for this property type
     */
    public int intValue()
    {
        return type;
    }

    public abstract Object getValue( PropertyRecord record, PropertyStore store );

    public static PropertyType getPropertyType( int type, boolean nullOnIllegal )
    {
        switch ( type )
        {
        case 0:
            if ( nullOnIllegal ) return null;
            break;
        case 1:
            return INT;
        case 2:
            return STRING;
        case 3:
            return BOOL;
        case 4:
            return DOUBLE;
        case 5:
            return FLOAT;
        case 6:
            return LONG;
        case 7:
            return BYTE;
        case 8:
            return CHAR;
        case 9:
            return ARRAY;
        case 10:
            return SHORT;
        case 11:
            return SHORT_STRING;
        }
        throw new InvalidRecordException( "Unknown property type:" + type );
    }
}
/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.server.rest.repr;

public abstract class MappingWriter
{
    MappingWriter newMapping( RepresentationType type, String param )
    {
        return newMapping( type.valueName, param );
    }

    protected boolean isInteractive()
    {
        return false;
    }

    protected abstract MappingWriter newMapping( String type, String key );

    ListWriter newList( RepresentationType type, String param )
    {
        if ( type.valueName == "map" ) {
        	return newList( type.listName, param );
        }
        if ( type.listName == null ) {
            throw new IllegalStateException( "Invalid list type: " + type );
        }
        return newList( type.listName, param );
    }

    protected abstract ListWriter newList( String type, String key );

    protected void writeString( String key, String value )
    {
        writeValue( RepresentationType.STRING, key, value );
    }

    void writeInteger( RepresentationType type, String param, long property )
    {
        writeInteger( type.valueName, param, property );
    }

    @SuppressWarnings( "boxing" )
    protected void writeInteger( String type, String key, long value )
    {
        writeValue( type, key, value );
    }

    void writeFloatingPointNumber( RepresentationType type, String key, double value )
    {
        writeFloatingPointNumber( type.valueName, key, value );
    }

    @SuppressWarnings( "boxing" )
    protected void writeFloatingPointNumber( String type, String key, double value )
    {
        writeValue( type, key, value );
    }

    @SuppressWarnings( "boxing" )
    protected void writeBoolean( String key, boolean value )
    {
        writeValue( RepresentationType.BOOLEAN, key, value );
    }

    void writeValue( RepresentationType type, String key, Object value )
    {
        writeValue( type.valueName, key, value );
    }

    protected abstract void writeValue( String type, String key, Object value );

    protected abstract void done();
}

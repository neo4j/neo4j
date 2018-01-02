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

public abstract class ListWriter
{
    MappingWriter newMapping( RepresentationType type )
    {
        return newMapping( type.valueName );
    }

    protected abstract MappingWriter newMapping( String type );

    ListWriter newList( RepresentationType type )
    {
        if ( type.listName == null )
            throw new IllegalStateException( "Invalid list type: " + type );
        return newList( type.listName );
    }

    protected abstract ListWriter newList( String type );

    protected void writeString( String value )
    {
        writeValue( RepresentationType.STRING, value );
    }

    @SuppressWarnings( "boxing" )
    protected void writeBoolean( boolean value )
    {
        writeValue( RepresentationType.BOOLEAN, value );
    }

    void writeInteger( RepresentationType type, long value )
    {
        writeInteger( type.valueName, value );
    }

    @SuppressWarnings( "boxing" )
    protected void writeInteger( String type, long value )
    {
        writeValue( type, value );
    }

    void writeFloatingPointNumber( RepresentationType type, double value )
    {
        writeFloatingPointNumber( type.valueName, value );
    }

    @SuppressWarnings( "boxing" )
    protected void writeFloatingPointNumber( String type, double value )
    {
        writeValue( type, value );
    }

    void writeValue( RepresentationType type, Object value )
    {
        writeValue( type.valueName, value );
    }

    protected abstract void writeValue( String type, Object value );

    protected abstract void done();
}

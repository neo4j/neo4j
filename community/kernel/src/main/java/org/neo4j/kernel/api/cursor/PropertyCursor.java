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
package org.neo4j.kernel.api.cursor;

import org.neo4j.cursor.Cursor;
import org.neo4j.function.Function;
import org.neo4j.function.ToIntFunction;
import org.neo4j.kernel.api.properties.DefinedProperty;

/**
 * Cursor for iterating over the properties of a node or relationship.
 */
public interface PropertyCursor
        extends Cursor
{
    Function<PropertyCursor, DefinedProperty> GET_PROPERTY = new Function<PropertyCursor, DefinedProperty>()
    {
        @Override
        public DefinedProperty apply( PropertyCursor propertyCursor )
        {
            return propertyCursor.getProperty();
        }
    };

    ToIntFunction<PropertyCursor> GET_KEY_INDEX_ID = new ToIntFunction<PropertyCursor>()
    {
        @Override
        public int apply( PropertyCursor value )
        {
            return value.getProperty().propertyKeyId();
        }
    };

    PropertyCursor EMPTY = new PropertyCursor()
    {
        @Override
        public boolean seek( int keyId )
        {
            return false;
        }

        @Override
        public DefinedProperty getProperty()
        {
            throw new IllegalStateException();
        }

        @Override
        public boolean next()
        {
            return false;
        }

        @Override
        public void close()
        {

        }
    };


    /**
     * Move the cursor to a particular property.
     *
     * @param keyId of the property to find
     * @return true if found
     */
    boolean seek( int keyId );

    DefinedProperty getProperty();
}

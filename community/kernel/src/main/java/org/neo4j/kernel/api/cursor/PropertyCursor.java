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

import java.nio.channels.WritableByteChannel;

import org.neo4j.cursor.Cursor;
import org.neo4j.function.ToIntFunction;

/**
 * Cursor for iterating over the properties of a node or relationship.
 */
public interface PropertyCursor
        extends Cursor
{
    ToIntFunction<PropertyCursor> GET_KEY_INDEX_ID = new ToIntFunction<PropertyCursor>()
    {
        @Override
        public int apply( PropertyCursor cursor )
        {
            return cursor.propertyKeyId();
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
        public int propertyKeyId()
        {
            throw new IllegalStateException();
        }

        @Override
        public Object value()
        {
            throw new IllegalStateException();
        }

        @Override
        public boolean booleanValue()
        {
            throw new IllegalStateException();
        }

        @Override
        public long longValue()
        {
            throw new IllegalStateException();
        }

        @Override
        public double doubleValue()
        {
            throw new IllegalStateException();
        }

        @Override
        public String stringValue()
        {
            throw new IllegalStateException();
        }

        @Override
        public void propertyData( WritableByteChannel channel )
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

    boolean seek( int keyId );

    int propertyKeyId();

    Object value();

    boolean booleanValue();

    long longValue();

    double doubleValue();

    String stringValue();

    void propertyData( WritableByteChannel channel );
}

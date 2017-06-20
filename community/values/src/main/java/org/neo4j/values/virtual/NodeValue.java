/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.values.virtual;

import org.neo4j.values.AnyValueWriter;
import org.neo4j.values.TextValue;

import static java.lang.String.format;

public class NodeValue extends VirtualNodeValue
{
    private final long id;
    private final TextValue[] labels;
    private final MapValue properties;

    NodeValue( long id, TextValue[] labels, MapValue properties )
    {
        assert labels != null;
        assert properties != null;

        this.id = id;
        this.labels = labels;
        this.properties = properties;
    }

    TextValue[] labels()
    {
        return labels;
    }

    MapValue properties()
    {
        return properties;
    }

    @Override
    public <E extends Exception> void writeTo( AnyValueWriter<E> writer ) throws E
    {
       writer.writeNode( id, labels, properties  );
    }

    @Override
    public long id()
    {
        return id;
    }

    @Override
    public String toString()
    {
        return format( "(%d)", id );
    }

}

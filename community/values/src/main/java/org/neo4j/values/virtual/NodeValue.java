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
package org.neo4j.values.virtual;


import org.neo4j.values.AnyValueWriter;
import org.neo4j.values.storable.TextArray;

import static java.lang.String.format;

public abstract class NodeValue extends VirtualNodeValue
{
    private final long id;

    protected NodeValue( long id )
    {
        this.id = id;
    }

    public abstract TextArray labels();

    public abstract MapValue properties();

    @Override
    public <E extends Exception> void writeTo( AnyValueWriter<E> writer ) throws E
    {
        writer.writeNode( id, labels(), properties() );
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

    @Override
    public String getTypeName()
    {
        return "Node";
    }

    static class DirectNodeValue extends NodeValue
    {
        private final TextArray labels;
        private final MapValue properties;

        DirectNodeValue( long id, TextArray labels, MapValue properties )
        {
            super( id );
            assert labels != null;
            assert properties != null;
            this.labels = labels;
            this.properties = properties;
        }

        @Override
        public TextArray labels()
        {
            return labels;
        }

        @Override
        public MapValue properties()
        {
            return properties;
        }
    }
}

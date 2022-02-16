/*
 * Copyright (c) "Neo4j"
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
import org.neo4j.values.ElementIdMapper;
import org.neo4j.values.storable.TextArray;

import static java.lang.String.format;
import static org.neo4j.memory.HeapEstimator.shallowSizeOfInstance;
import static org.neo4j.values.AnyValueWriter.EntityMode.REFERENCE;

public abstract class NodeValue extends VirtualNodeValue
{
    private final long id;
    private final ElementIdMapper elementIdMapper;

    protected NodeValue( long id, ElementIdMapper elementIdMapper )
    {
        this.id = id;
        this.elementIdMapper = elementIdMapper;
    }

    public abstract TextArray labels();

    public abstract MapValue properties();

    @Override
    public <E extends Exception> void writeTo( AnyValueWriter<E> writer ) throws E
    {
        if ( writer.entityMode() == REFERENCE )
        {
            writer.writeNodeReference( id );
        }
        else
        {
            writer.writeNode( id, labels(), properties(), isDeleted() );
        }
    }

    @Override
    public long id()
    {
        return id;
    }

    @Override
    public String elementId()
    {
        return elementIdMapper.nodeElementId( id );
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

    @Override
    ElementIdMapper elementIdMapper()
    {
        return elementIdMapper;
    }

    private static final long DIRECT_NODE_SHALLOW_SIZE = shallowSizeOfInstance( DirectNodeValue.class );

    static class DirectNodeValue extends NodeValue
    {
        private final TextArray labels;
        private final MapValue properties;
        private final boolean isDeleted;
        private final String elementId;

        /**
         * @param id internal id of the node.
         * @param elementId element id of the node, or {@code null} which means it will be created from the {@code elementIdMapper} only when requested.
         * @param elementIdMapper mapping internal id to element id.
         * @param labels label names of this node.
         * @param properties properties of this node.
         * @param isDeleted whether this node is deleted.
         */
        DirectNodeValue( long id, String elementId, ElementIdMapper elementIdMapper, TextArray labels, MapValue properties, boolean isDeleted )
        {
            super( id, elementIdMapper );
            this.elementId = elementId;
            assert labels != null;
            assert properties != null;
            this.labels = labels;
            this.properties = properties;
            this.isDeleted = isDeleted;
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

        @Override
        public long estimatedHeapUsage()
        {
            return DIRECT_NODE_SHALLOW_SIZE + labels.estimatedHeapUsage() + properties.estimatedHeapUsage();
        }

        @Override
        public boolean isDeleted()
        {
            return isDeleted;
        }

        @Override
        public String elementId()
        {
            return elementId != null ? elementId : super.elementId();
        }
    }
}

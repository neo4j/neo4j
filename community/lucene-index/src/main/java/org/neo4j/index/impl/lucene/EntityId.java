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
package org.neo4j.index.impl.lucene;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;

import java.util.Collection;

import org.neo4j.graphdb.Relationship;

/**
 * Represents id data about an entity that is to be indexed.
 */
public interface EntityId
{
    /**
     * @return the entity id.
     */
    long id();

    /**
     * Enhances a {@link Document}, adding more id data to it if necessary.
     * @param document the {@link Document} to enhance.
     */
    void enhance( Document document );

    abstract class AbstractData implements EntityId
    {
        protected long id;

        AbstractData( long id )
        {
            this.id = id;
        }

        @Override
        public long id()
        {
            return id;
        }

        @Override
        public boolean equals( Object obj )
        {
            return obj instanceof EntityId ? ((EntityId)obj).id() == id : false;
        }

        @Override
        public int hashCode()
        {
            return (int) ((id >>> 32) ^ id);
        }
    }

    /**
     * {@link EntityId} only carrying entity id.
     */
    class IdData extends AbstractData
    {
        public IdData( long id )
        {
            super( id );
        }

        @Override
        public void enhance( Document document )
        {   // Nothing to enhance here
        }
    }

    /**
     * {@link EntityId} including additional start/end node for {@link Relationship relationships}.
     */
    class RelationshipData extends AbstractData
    {
        private final long startNode;
        private final long endNode;

        public RelationshipData( long id, long startNode, long endNode )
        {
            super( id );
            this.startNode = startNode;
            this.endNode = endNode;
        }

        @Override
        public void enhance( Document document )
        {
            document.add( new Field( LuceneIndex.KEY_START_NODE_ID, "" + startNode, Store.YES,
                    org.apache.lucene.document.Field.Index.NOT_ANALYZED ) );
            document.add( new Field( LuceneIndex.KEY_END_NODE_ID, "" + endNode, Store.YES,
                    org.apache.lucene.document.Field.Index.NOT_ANALYZED ) );
        }
    }

    /**
     * Used in {@link Collection#contains(Object)} and {@link Collection#remove(Object)} f.ex. to save
     * object allocations where you have primitive {@code long} ids and want to call those methods
     * on a {@link Collection} containing {@link EntityId} instances.
     */
    class LongCostume extends IdData
    {
        public LongCostume()
        {
            super( -1 );
        }

        public LongCostume setId( long id )
        {
            this.id = id;
            return this;
        }
    }
}

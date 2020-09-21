/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.storageengine.api.txstate;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableLong;
import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.api.set.primitive.LongSet;

import java.util.function.Consumer;
import java.util.function.Predicate;

import org.neo4j.storageengine.api.RelationshipDirection;
import org.neo4j.storageengine.api.RelationshipVisitor;
import org.neo4j.util.Preconditions;

/**
 * <pre>
 * // For example
 * ids.forEachSplit( byNode ->
 * {
 *     // To access ids for each chain (by node, relationship type and direction) individually
 *     byNode.forEachSplit( byNodeAndType ->
 *     {
 *         byNodeAndType.out().forEach( id -> {} ); // all relationship ids for the OUTGOING relationship chain for this node and type
 *         byNodeAndType.in().forEach( id -> {} );  // all relationship ids for the INCOMING relationship chain for this node and type
 *         byNodeAndType.loop().forEach( id -> {} );// all relationship ids for the LOOP relationship chain for this node and type
 *     } );
 *
 *     // To access all ids for this node
 *     byNode.forEach( id -> {} ); // all relationship ids for this node
 * } );
 * </pre>
 */
public interface RelationshipModifications
{
    RelationshipBatch EMPTY_BATCH = new RelationshipBatch()
    {
        @Override
        public int size()
        {
            return 0;
        }

        @Override
        public void forEach( RelationshipVisitor relationship )
        {
        }
    };

    static IdDataDecorator noAdditionalDataDecorator()
    {
        return new IdDataDecorator()
        {
            @Override
            public <E extends Exception> void accept( long id, RelationshipVisitor<E> visitor ) throws E
            {
                visitor.visit( id, -1, -1, -1 );
            }
        };
    }

    static RelationshipBatch idsAsBatch( LongSet ids )
    {
        return idsAsBatch( ids, noAdditionalDataDecorator() );
    }

    static RelationshipBatch idsAsBatch( LongSet ids, IdDataDecorator idDataDecorator )
    {
        return new RelationshipBatch()
        {
            @Override
            public int size()
            {
                return ids.size();
            }

            @Override
            public boolean isEmpty()
            {
                return ids.isEmpty();
            }

            @Override
            public boolean contains( long id )
            {
                return ids.contains( id );
            }

            @Override
            public long first()
            {
                return ids.longIterator().next();
            }

            @Override
            public <E extends Exception> void forEach( RelationshipVisitor<E> relationship ) throws E
            {
                LongIterator iterator = ids.longIterator();
                while ( iterator.hasNext() )
                {
                    long id = iterator.next();
                    idDataDecorator.accept( id, relationship );
                }
            }
        };
    }

    void forEachSplit( Consumer<NodeRelationshipIds> nodeRelationshipIds );

    RelationshipBatch creations();

    RelationshipBatch deletions();

    interface NodeRelationshipIds
    {
        long nodeId();

        boolean hasCreations();

        boolean hasCreations( int type );

        boolean hasDeletions();

        RelationshipBatch creations();

        RelationshipBatch deletions();

        default void forEachCreationSplit( Consumer<NodeRelationshipTypeIds> nodeRelationshipTypeIds )
        {
            forEachCreationSplitInterruptible( byType ->
            {
                nodeRelationshipTypeIds.accept( byType );
                return false;
            } );
        }

        void forEachCreationSplitInterruptible( Predicate<NodeRelationshipTypeIds> nodeRelationshipTypeIds );

        default void forEachDeletionSplit( Consumer<NodeRelationshipTypeIds> nodeRelationshipTypeIds )
        {
            forEachDeletionSplitInterruptible( byType ->
            {
                nodeRelationshipTypeIds.accept( byType );
                return false;
            } );
        }

        void forEachDeletionSplitInterruptible( Predicate<NodeRelationshipTypeIds> nodeRelationshipTypeIds );
    }

    interface NodeRelationshipTypeIds
    {
        int type();

        default RelationshipBatch ids( RelationshipDirection direction )
        {
            switch ( direction )
            {
            case OUTGOING:
                return out();
            case INCOMING:
                return in();
            case LOOP:
                return loop();
            default:
                throw new IllegalArgumentException( direction.name() );
            }
        }

        boolean hasOut();

        boolean hasIn();

        boolean hasLoop();

        RelationshipBatch out();

        RelationshipBatch in();

        RelationshipBatch loop();
    }

    interface RelationshipBatch
    {
        int size();

        <E extends Exception> void forEach( RelationshipVisitor<E> relationship ) throws E;

        // The default implementations below are inefficient, but are implemented like this for simplicity of test versions of this interface,
        // any implementor that is in production code will implement properly

        default boolean isEmpty()
        {
            return size() == 0;
        }

        default boolean contains( long id )
        {
            if ( isEmpty() )
            {
                return false;
            }
            MutableBoolean contains = new MutableBoolean();
            forEach( ( relationshipId, typeId, startNodeId, endNodeId ) ->
            {
                if ( relationshipId == id )
                {
                    contains.setTrue();
                }
            } );
            return contains.booleanValue();
        }

        default long first()
        {
            Preconditions.checkState( !isEmpty(), "No ids" );
            MutableLong first = new MutableLong( -1 );
            forEach( ( relationshipId, typeId, startNodeId, endNodeId ) ->
            {
                if ( first.longValue() == -1 )
                {
                    first.setValue( relationshipId );
                }
            } );
            return first.longValue();
        }
    }

    interface IdDataDecorator
    {
        <E extends Exception> void accept( long id, RelationshipVisitor<E> visitor ) throws E;
    }
}

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
package org.neo4j.kernel.impl.storageengine.impl.recordstorage;

import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.neo4j.kernel.impl.store.id.IdRange;
import org.neo4j.kernel.impl.store.id.IdSequence;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.transaction.state.RecordAccess;
import org.neo4j.kernel.impl.transaction.state.RecordChangeSet;

/** Test utility DSL for creating store records */
public class RecordBuilders
{
    public static <R extends AbstractBaseRecord, A> List<R> records(
            Iterable<RecordAccess.RecordProxy<R,A>> changes )
    {
        return StreamSupport.stream( changes.spliterator(), false ).map(
                RecordAccess.RecordProxy::forChangingData ).collect( Collectors.toList() );
    }

    public static NodeRecord node( long id, Consumer<NodeRecord>... modifiers )
    {
        NodeRecord record = new NodeRecord( id );
        record.initialize( true, Record.NO_NEXT_PROPERTY.intValue(), false,
                Record.NO_NEXT_RELATIONSHIP.intValue(), Record.NO_LABELS_FIELD.intValue() );
        for ( Consumer<NodeRecord> modifier : modifiers )
        {
            modifier.accept( record );
        }

        return record;
    }

    public static RelationshipRecord rel( long id, Consumer<RelationshipRecord>... modifiers )
    {
        RelationshipRecord record = new RelationshipRecord( id );
        record.initialize( true, Record.NO_NEXT_PROPERTY.intValue(), -1, -1, 0,
                Record.NO_PREV_RELATIONSHIP.longValue(), Record.NO_NEXT_RELATIONSHIP.longValue(),
                Record.NO_PREV_RELATIONSHIP.longValue(), Record.NO_NEXT_RELATIONSHIP.longValue(),
                true, true );
        for ( Consumer<RelationshipRecord> modifier : modifiers )
        {
            modifier.accept( record );
        }

        return record;
    }

    public static RelationshipGroupRecord relGroup( long id, Consumer<RelationshipGroupRecord>... modifiers )
    {
        RelationshipGroupRecord record = new RelationshipGroupRecord( id );
        record.initialize( true, 0, Record.NO_NEXT_RELATIONSHIP.longValue(),
                Record.NO_NEXT_RELATIONSHIP.longValue(),
                Record.NO_NEXT_RELATIONSHIP.longValue(), -1,
                Record.NO_NEXT_RELATIONSHIP.longValue() );
        for ( Consumer<RelationshipGroupRecord> modifier : modifiers )
        {
            modifier.accept( record );
        }

        return record;
    }

    // Below is a set of static "modifier" functions, that are meant to act as a DSL for building
    // records. It's a first stab at it - it has the clear issue that currently each function name
    // can only map to one record type (since the Consumers are typed), which means field names
    // class. Refactor as needed!

    public static Consumer<NodeRecord> nextRel( long nextRelId )
    {
        return n -> n.setNextRel( nextRelId );
    }

    public static Consumer<NodeRecord> group( long groupId )
    {
        return n -> {
            n.setDense( true );
            n.setNextRel( groupId );
        };
    }

    public static Consumer<RelationshipRecord> from( long fromNodeId )
    {
        return n -> n.setFirstNode( fromNodeId );
    }

    public static Consumer<RelationshipRecord> to( long toNodeId )
    {
        return n -> n.setSecondNode( toNodeId );
    }

    public static Consumer<RelationshipRecord> sCount( long count )
    {
        return n ->
        {
            n.setFirstInFirstChain( true );
            n.setFirstPrevRel( count );
        };
    }

    public static Consumer<RelationshipRecord> tCount( long count )
    {
        return n ->
        {
            n.setFirstInSecondChain( true );
            n.setSecondPrevRel( count );
        };
    }

    public static Consumer<RelationshipRecord> sPrev( long id )
    {
        return n ->
        {
            n.setFirstInFirstChain( false );
            n.setFirstPrevRel( id );
        };
    }

    public static Consumer<RelationshipRecord> sNext( long id )
    {
        return n -> n.setFirstNextRel( id );
    }

    public static Consumer<RelationshipRecord> tPrev( long id )
    {
        return n ->
        {
            n.setFirstInSecondChain( false );
            n.setSecondPrevRel( id );
        };
    }

    public static Consumer<RelationshipRecord> tNext( long id )
    {
        return n -> n.setSecondNextRel( id );
    }

    public static Consumer<RelationshipGroupRecord> firstLoop( long id )
    {
        return g -> g.setFirstLoop( id );
    }

    public static Consumer<RelationshipGroupRecord> firstOut( long id )
    {
        return g -> g.setFirstOut( id );
    }

    public static Consumer<RelationshipGroupRecord> firstIn( long id )
    {
        return g -> g.setFirstIn( id );
    }

    public static Consumer<RelationshipGroupRecord> owningNode( long id )
    {
        return g -> g.setOwningNode( id );
    }

    public static <R> Stream<R> filterType( Object[] in, Class<R> type )
    {
        return filterType( Stream.of( in ), type );
    }

    public static <R> Stream<R> filterType( Stream<?> in, Class<R> type )
    {
        return in.filter( type::isInstance ).map( type::cast );
    }

    public static RecordChangeSet newChangeSet( AbstractBaseRecord... records )
    {
        return new RecordChangeSet(
                new Loader( filterType( records, NodeRecord.class ).collect( Collectors.toList() ),
                        (BiFunction<Long,Object,NodeRecord>) ( key, extra ) -> new NodeRecord( key ) ),
                null,
                new Loader( filterType( records, RelationshipRecord.class ).collect( Collectors.toList() ),
                        (BiFunction<Long,Object,RelationshipRecord>) ( key, extra ) -> new RelationshipRecord( key ) ),
                new Loader( filterType( records, RelationshipGroupRecord.class ).collect( Collectors.toList() ),
                        (BiFunction<Long,Integer,RelationshipGroupRecord>) ( key, extra ) -> {
                            RelationshipGroupRecord group =
                                    new RelationshipGroupRecord( key );
                            group.setType( extra );
                            return group;
                        } ),
                null, null, null, null );
    }

    public static RelationshipGroupGetter newRelGroupGetter( AbstractBaseRecord... records )
    {
        return new RelationshipGroupGetter( new IdSequence()
        {
            private long nextId = filterType( records, RelationshipGroupRecord.class ).count();

            @Override
            public long nextId()
            {
                return nextId++;
            }

            @Override
            public IdRange nextIdBatch( int size )
            {
                throw new UnsupportedOperationException();
            }
        } );
    }

    private static class Loader<T extends AbstractBaseRecord, E> implements RecordAccess.Loader<T,E>
    {
        private final List<T> records;
        private final BiFunction<Long, E, T> newRecord;

        Loader( List<T> records, BiFunction<Long,E,T> newRecord )
        {
            this.records = records;
            this.newRecord = newRecord;
        }

        @Override
        public T newUnused( long key, E additionalData )
        {
            return newRecord.apply( key, additionalData );
        }

        @Override
        public T load( long key, E additionalData )
        {
            return records.stream().filter( r -> r.getId() == key ).findFirst().get();
        }

        @Override
        public void ensureHeavy( T relationshipRecord )
        {

        }

        @SuppressWarnings( "unchecked" )
        @Override
        public T clone( T record )
        {
            return (T)record.clone();
        }
    }
}

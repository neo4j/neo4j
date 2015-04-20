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
package org.neo4j.kernel.impl.store;

import java.io.File;
import java.util.Collection;
import java.util.Iterator;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.helpers.Predicate;
import org.neo4j.helpers.collection.IterableWrapper;
import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.helpers.progress.ProgressListener;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.impl.store.id.IdSequence;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;

public interface RecordStore<R extends AbstractBaseRecord> extends IdSequence
{
    File getStorageFileName();

    long getHighId();

    long getHighestPossibleIdInUse();

    R getRecord( long id );

    Long getNextRecordReference( R record );

    Collection<R> getRecords( long id );

    void updateRecord( R record );

    R forceGetRecord( long id );

    R forceGetRaw( R record );

    R forceGetRaw( long id );

    void forceUpdateRecord( R record );

    <FAILURE extends Exception> void accept( Processor<FAILURE> processor, R record ) throws FAILURE;

    int getRecordSize();

    int getRecordHeaderSize();

    void close();

    int getNumberOfReservedLowIds();

    Predicate<AbstractBaseRecord> IN_USE = new Predicate<AbstractBaseRecord>()
    {
        @Override
        public boolean accept( AbstractBaseRecord item )
        {
            return item.inUse();
        }
    };

    @SuppressWarnings( "unchecked" )
    abstract class Processor<FAILURE extends Exception>
    {
        // Have it volatile so that it can be stopped from a different thread.
        private volatile boolean shouldStop;

        public void stop()
        {
            shouldStop = true;
        }

        public abstract void processSchema( RecordStore<DynamicRecord> store, DynamicRecord schema ) throws FAILURE;

        public abstract void processNode( RecordStore<NodeRecord> store, NodeRecord node ) throws FAILURE;

        public abstract void processRelationship( RecordStore<RelationshipRecord> store, RelationshipRecord rel )
                throws FAILURE;

        public abstract void processProperty( RecordStore<PropertyRecord> store, PropertyRecord property ) throws
                FAILURE;

        public abstract void processString( RecordStore<DynamicRecord> store, DynamicRecord string, IdType idType )
                throws FAILURE;

        public abstract void processArray( RecordStore<DynamicRecord> store, DynamicRecord array ) throws FAILURE;

        public abstract void processLabelArrayWithOwner( RecordStore<DynamicRecord> store, DynamicRecord labelArray )
                throws FAILURE;

        public abstract void processRelationshipTypeToken( RecordStore<RelationshipTypeTokenRecord> store,
                RelationshipTypeTokenRecord record ) throws FAILURE;

        public abstract void processPropertyKeyToken( RecordStore<PropertyKeyTokenRecord> store, PropertyKeyTokenRecord
                record ) throws FAILURE;

        public abstract void processLabelToken( RecordStore<LabelTokenRecord> store, LabelTokenRecord record ) throws
                FAILURE;

        public abstract void processRelationshipGroup( RecordStore<RelationshipGroupRecord> store,
                RelationshipGroupRecord record ) throws FAILURE;

        protected <R extends AbstractBaseRecord> R getRecord( RecordStore<R> store, long id )
        {
            return store.forceGetRecord( id );
        }

        public <R extends AbstractBaseRecord> void applyById( RecordStore<R> store, Iterable<Long> ids ) throws FAILURE
        {
            for ( R record : Scanner.scanById( store, ids ) )
            {
                store.accept( this, record );
            }
        }

        public <R extends AbstractBaseRecord> void applyFiltered( RecordStore<R> store,
                Predicate<? super R>... filters ) throws FAILURE
        {
            apply( store, ProgressListener.NONE, filters );
        }

        public <R extends AbstractBaseRecord> void applyFiltered( RecordStore<R> store,
                ProgressListener progressListener,
                Predicate<? super R>... filters ) throws FAILURE
        {
            apply( store, progressListener, filters );
        }

        private <R extends AbstractBaseRecord> void apply( RecordStore<R> store, ProgressListener progressListener,
                Predicate<? super R>... filters ) throws FAILURE
        {
            for ( R record : Scanner.scan( store, filters ) )
            {
                if ( shouldStop )
                {
                    break;
                }

                store.accept( this, record );
                progressListener.set( record.getLongId() );
            }
            progressListener.done();
        }
    }

    static class Scanner
    {
        @SafeVarargs
        public static <R extends AbstractBaseRecord> Iterable<R> scan( final RecordStore<R> store,
                final Predicate<? super R>... filters )
        {
            return new Iterable<R>()
            {
                @Override
                public Iterator<R> iterator()
                {
                    return new PrefetchingIterator<R>()
                    {
                        final PrimitiveLongIterator ids = new StoreIdIterator( store );

                        @Override
                        protected R fetchNextOrNull()
                        {
                            scan:
                            while ( ids.hasNext() )
                            {
                                R record = store.forceGetRecord( ids.next() );
                                for ( Predicate<? super R> filter : filters )
                                {
                                    if ( !filter.accept( record ) )
                                    {
                                        continue scan;
                                    }
                                }
                                return record;
                            }
                            return null;
                        }
                    };
                }
            };
        }

        public static <R extends AbstractBaseRecord> Iterable<R> scanById( final RecordStore<R> store,
                Iterable<Long> ids )
        {
            return new IterableWrapper<R,Long>( ids )
            {
                @Override
                protected R underlyingObjectToObject( Long id )
                {
                    return store.forceGetRecord( id );
                }
            };
        }
    }
}

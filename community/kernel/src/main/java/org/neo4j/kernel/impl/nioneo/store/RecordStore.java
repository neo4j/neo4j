/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.kernel.impl.nioneo.store;

import java.io.PrintStream;
import java.util.Iterator;

import org.neo4j.helpers.Predicate;
import org.neo4j.helpers.collection.IterableWrapper;
import org.neo4j.helpers.collection.PrefetchingIterator;

public interface RecordStore<R extends AbstractBaseRecord>
{
    public long getHighId();

    public R getRecord( long id );

    public void updateRecord( R record );

    public R forceGetRecord( long id );

    public void forceUpdateRecord( R record );

    public void accept( Processor processor, R record );

    public int getRecordSize();

    public int getRecordHeaderSize();

    public void close();

    public static final Predicate<AbstractBaseRecord> IN_USE = new Predicate<AbstractBaseRecord>()
    {
        @Override
        public boolean accept( AbstractBaseRecord item )
        {
            return item.inUse();
        }
    };

    public static abstract class Processor
    {
        public void processNode( RecordStore<NodeRecord> store, NodeRecord node )
        {
            processRecord( NodeRecord.class, store, node );
        }

        public void processRelationship( RecordStore<RelationshipRecord> store, RelationshipRecord rel )
        {
            processRecord( RelationshipRecord.class, store, rel );
        }

        public void processProperty( RecordStore<PropertyRecord> store, PropertyRecord property )
        {
            processRecord( PropertyRecord.class, store, property );
        }

        public void processString( RecordStore<DynamicRecord> store, DynamicRecord string )
        {
            processDynamic( store, string );
        }

        public void processArray( RecordStore<DynamicRecord> store, DynamicRecord array )
        {
            processDynamic( store, array );
        }

        protected void processDynamic( RecordStore<DynamicRecord> store, DynamicRecord record )
        {
            processRecord( DynamicRecord.class, store, record );
        }

        public void processRelationshipType( RecordStore<RelationshipTypeRecord> store, RelationshipTypeRecord record )
        {
            processRecord( RelationshipTypeRecord.class, store, record );
        }

        public void processPropertyIndex( RecordStore<PropertyIndexRecord> store, PropertyIndexRecord record )
        {
            processRecord( PropertyIndexRecord.class, store, record );
        }

        protected <R extends AbstractBaseRecord> void processRecord( Class<R> type, RecordStore<R> store, R record )
        {
            throw new UnsupportedOperationException( this + " does not process "
                                                     + type.getSimpleName().replace( "Record", "" ) + " records" );
        }

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
                        final long highId = store.getHighId();
                        int id = 0;

                        @Override
                        protected R fetchNextOrNull()
                        {
                            scan: while ( id <= highId && id >= 0 )
                            {
                                R record = store.forceGetRecord( id++ );
                                for ( Predicate<? super R> filter : filters )
                                {
                                    if ( !filter.accept( record ) ) continue scan;
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
            return new IterableWrapper<R, Long>( ids )
            {
                @Override
                protected R underlyingObjectToObject( Long id )
                {
                    return store.forceGetRecord( id.longValue() );
                }
            };
        }

        public <R extends AbstractBaseRecord> void applyById( RecordStore<R> store, Iterable<Long> ids )
        {
            for ( R record : scanById( store, ids ) )
                store.accept( this, record );
        }

        public <R extends AbstractBaseRecord> void applyFiltered( RecordStore<R> store, Predicate<? super R>... filters )
        {
            apply( store, null, filters );
        }

        public <R extends AbstractBaseRecord> void applyWithTerminalProgress( RecordStore<R> store,
                Predicate<? super R>... filters )
        {
            apply( store, System.err, filters );
        }

        private final <R extends AbstractBaseRecord> void apply( RecordStore<R> store, final PrintStream out,
                Predicate<? super R>... filters )
        {
            long highId = store.getHighId();
            if ( out != null ) out.printf( "%s for %s records%n", this, Long.toString( highId ) );
            int lastPermille = 0;
            for ( R record : scan( store, filters ) )
            {
                store.accept( this, record );
                if ( out != null )
                {
                    int permille = (int) ( ( record.getLongId() * 1000L ) / highId );
                    if ( permille != lastPermille ) progress( out, lastPermille = permille );
                }
            }
            if ( out != null ) if ( lastPermille != 1000 ) progress( out, 1000 );
        }

        private static void progress( PrintStream out, int permille )
        {
            if ( permille % 100 == 0 )
                out.printf( "%3s%%%n", Integer.toString( permille / 10 ) );
            else if ( permille % 5 == 0 ) out.print( "." );
        }
    }
}

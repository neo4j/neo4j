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
package org.neo4j.kernel.impl.store;

import java.io.File;
import java.util.Collection;
import java.util.function.Predicate;

import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.helpers.progress.ProgressListener;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.store.id.IdRange;
import org.neo4j.kernel.impl.store.id.IdSequence;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;

/**
 * A store for {@link #updateRecord(AbstractBaseRecord) updating} and
 * {@link #getRecord(long, AbstractBaseRecord, RecordLoad) getting} records.
 *
 * There are two ways of getting records, either one-by-one using
 * {@link #getRecord(long, AbstractBaseRecord, RecordLoad)}, passing in record retrieved from {@link #newRecord()}.
 * This to make a conscious decision about who will create the record instance and in that process figure out
 * ways to reduce number of record instances created. The other way is to use a {@link RecordCursor}, created
 * by {@link #newRecordCursor(AbstractBaseRecord)} and placed at a certain record using
 * {@link #placeRecordCursor(long, RecordCursor, RecordLoad)}. A {@link RecordCursor} will keep underlying
 * {@link PageCursor} open until until the {@link RecordCursor} is closed and so will be efficient if multiple
 * records are retrieved from it. A {@link RecordCursor} will follow {@link #getNextRecordReference(AbstractBaseRecord)}
 * references to get to {@link RecordCursor#next()} record.
 *
 * @param <RECORD> type of {@link AbstractBaseRecord}.
 */
public interface RecordStore<RECORD extends AbstractBaseRecord> extends IdSequence
{
    /**
     * @return the {@link File} that backs this store.
     */
    File getStorageFileName();

    /**
     * @return high id of this store, i.e an id higher than any in use record.
     */
    long getHighId();

    /**
     * @return highest id in use in this store.
     */
    long getHighestPossibleIdInUse();

    /**
     * Sets highest id in use for this store. This is for when records are applied to this store where
     * the ids have been generated through some other means. Having an up to date highest possible id
     * makes sure that closing this store truncates at the right place and that "all record scans" can
     * see all records.
     *
     * @param highestIdInUse highest id that is now in use in this store.
     */
    void setHighestPossibleIdInUse( long highestIdInUse );

    /**
     * @return a new record instance for receiving data by {@link #getRecord(long, AbstractBaseRecord, RecordLoad)}
     * and {@link #newRecordCursor(AbstractBaseRecord)}.
     */
    RECORD newRecord();

    /**
     * Reads a record from the store into {@code target}. Depending on {@link RecordLoad} given there will
     * be different behavior, although the {@code target} record will be marked with the specified
     * {@code id} after participating in this method call.
     * <ul>
     * <li>{@link RecordLoad#CHECK}: As little data as possible is read to determine whether or not the record
     *     is in use. If not in use then no more data will be loaded into the target record and
     *     the the data of the record will be {@link AbstractBaseRecord#clear() cleared}.</li>
     * <li>{@link RecordLoad#NORMAL}: Just like {@link RecordLoad#CHECK}, but with the difference that
     *     an {@link InvalidRecordException} will be thrown if the record isn't in use.</li>
     * <li>{@link RecordLoad#FORCE}: The entire contents of the record will be loaded into the target record
     *     regardless if the record is in use or not. This leaves no guarantees about the data in the record
     *     after this method call, except that the id will be the specified {@code id}.
     *
     * @param id the id of the record to load.
     * @param target record where data will be loaded into. This record will have its id set to the specified
     * {@code id} as part of this method call.
     * @param mode loading behaviour, read more in method description.
     * @return the record that was passed in, for convenience.
     * @throws InvalidRecordException if record not in use and the {@code mode} is allows for throwing.
     */
    RECORD getRecord( long id, RECORD target, RecordLoad mode ) throws InvalidRecordException;

    /**
     * For stores that have other stores coupled underneath, the "top level" record will have a flag
     * saying whether or not it's light. Light means that no records from the coupled store have been loaded yet.
     * This method can load those records and enrich the target record with those, marking it as heavy.
     *
     * @param record record to make heavy, if not already.
     */
    void ensureHeavy( RECORD record );

    /**
     * Reads records that belong together, a chain of records that as a whole forms the entirety of a data item.
     *
     * @param firstId record id of the first record to start loading from.
     * @param mode {@link RecordLoad} mode.
     * @return {@link Collection} of records in the loaded chain.
     * @throws InvalidRecordException if some record not in use and the {@code mode} is allows for throwing.
     */
    Collection<RECORD> getRecords( long firstId, RecordLoad mode ) throws InvalidRecordException;

    /**
     * Instantiates a new record cursor capable of iterating over records in this store. A {@link RecordCursor}
     * gets created with one record and will use every time it reads records.
     *
     * @param record instance to use when reading record data.
     * @return a new {@link RecordCursor} instance capable of reading records in this store.
     */
    RecordCursor<RECORD> newRecordCursor( RECORD record );

    /**
     * Returns another record id which the given {@code record} references and which a {@link RecordCursor}
     * would follow and read next.
     *
     * @param record to read the "next" reference from.
     * @return record id of "next" record that the given {@code record} references, or {@link Record#NULL_REFERENCE}
     * if the record doesn't reference a next record.
     */
    long getNextRecordReference( RECORD record );

    /**
     * Updates this store with the contents of {@code record} at the record id
     * {@link AbstractBaseRecord#getId() specified} by the record. The whole record will be written if
     * the given record is {@link AbstractBaseRecord#inUse() in use}, not necessarily so if it's not in use.
     *
     * @param record containing data to write to this store at the {@link AbstractBaseRecord#getId() id}
     * specified by the record.
     */
    void updateRecord( RECORD record );

    /**
     * Lets {@code record} be processed by {@link Processor}.
     *
     * @param processor {@link Processor} of records.
     * @param record to process.
     * @throws FAILURE if the processor fails.
     */
    <FAILURE extends Exception> void accept( Processor<FAILURE> processor, RECORD record ) throws FAILURE;

    /**
     * @return number of bytes each record in this store occupies. All records in a store is of the same size.
     */
    int getRecordSize();

    /**
     * @deprecated since it's exposed through the generic {@link RecordStore} interface although only
     * applicable to one particular type of of implementation of it.
     * @return record "data" size, only applicable to dynamic record stores where record size may be specified
     * at creation time and later used every time the store is opened. Data size refers to number of bytes
     * of a record without header information, such as "inUse" and "next".
     */
    @Deprecated
    int getRecordDataSize();

    /**
     * @return underlying storage is assumed to work with pages. This method returns number of records that
     * will fit into each page.
     */
    int getRecordsPerPage();

    /**
     * Closes this store and releases any resource attached to it.
     */
    void close();

    /**
     * Flushes all pending {@link #updateRecord(AbstractBaseRecord) updates} to underlying storage.
     * This call is blocking and will ensure all updates since last call to this method are durable
     * once the call returns.
     */
    void flush();

    /**
     * Some stores may have meta data stored in the header of the store file. Since all records in a store
     * are of the same size the means of storing that meta data is to occupy one or more records at the
     * beginning of the store (0...).
     *
     * @return the number of records in the beginning of the file that are reserved for header meta data.
     */
    int getNumberOfReservedLowIds();

    /**
     * Returns store header (see {@link #getNumberOfReservedLowIds()}) as {@code int}. Exposed like this
     * for convenience since all known store headers are ints.
     *
     * @return store header as an int value, e.g the first 4 bytes of the first (reserved) record in this store.
     */
    int getStoreHeaderInt();

    /**
     * Called once all changes to a record is ready to be converted into a command.
     *
     * @param record record to prepare, potentially updating it with more information before converting into a command.
     */
    void prepareForCommit( RECORD record );

    /**
     * Scan the given range of records both inclusive, and pass all the in-use ones to the given processor, one by one.
     *
     * The record passed to the NodeRecordScanner is reused instead of reallocated for every record, so it must be
     * cloned if you want to save it for later.
     * @param visitor {@link Visitor} notified about all records.
     * @throws Exception on error reading from store.
     */
    <EXCEPTION extends Exception> void scanAllRecords( Visitor<RECORD,EXCEPTION> visitor ) throws EXCEPTION;

    void freeId( long id );

    Predicate<AbstractBaseRecord> IN_USE = AbstractBaseRecord::inUse;

    class Delegator<R extends AbstractBaseRecord> implements RecordStore<R>
    {
        private final RecordStore<R> actual;

        @Override
        public void setHighestPossibleIdInUse( long highestIdInUse )
        {
            actual.setHighestPossibleIdInUse( highestIdInUse );
        }

        @Override
        public R newRecord()
        {
            return actual.newRecord();
        }

        @Override
        public R getRecord( long id, R target, RecordLoad mode ) throws InvalidRecordException
        {
            return actual.getRecord( id, target, mode );
        }

        @Override
        public Collection<R> getRecords( long firstId, RecordLoad mode ) throws InvalidRecordException
        {
            return actual.getRecords( firstId, mode );
        }

        @Override
        public RecordCursor<R> newRecordCursor( R record )
        {
            return actual.newRecordCursor( record );
        }

        @Override
        public long getNextRecordReference( R record )
        {
            return actual.getNextRecordReference( record );
        }

        public Delegator( RecordStore<R> actual )
        {
            this.actual = actual;
        }

        @Override
        public long nextId()
        {
            return actual.nextId();
        }

        @Override
        public IdRange nextIdBatch( int size )
        {
            return actual.nextIdBatch( size );
        }

        @Override
        public File getStorageFileName()
        {
            return actual.getStorageFileName();
        }

        @Override
        public long getHighId()
        {
            return actual.getHighId();
        }

        @Override
        public long getHighestPossibleIdInUse()
        {
            return actual.getHighestPossibleIdInUse();
        }

        @Override
        public void updateRecord( R record )
        {
            actual.updateRecord( record );
        }

        @Override
        public <FAILURE extends Exception> void accept( Processor<FAILURE> processor, R record ) throws FAILURE
        {
            actual.accept( processor, record );
        }

        @Override
        public int getRecordSize()
        {
            return actual.getRecordSize();
        }

        @Override
        public int getRecordDataSize()
        {
            return actual.getRecordDataSize();
        }

        @Override
        public int getRecordsPerPage()
        {
            return actual.getRecordsPerPage();
        }

        @Override
        public int getStoreHeaderInt()
        {
            return actual.getStoreHeaderInt();
        }

        @Override
        public void close()
        {
            actual.close();
        }

        @Override
        public int getNumberOfReservedLowIds()
        {
            return actual.getNumberOfReservedLowIds();
        }

        @Override
        public void flush()
        {
            actual.flush();
        }

        @Override
        public void ensureHeavy( R record )
        {
            actual.ensureHeavy( record );
        }

        @Override
        public void prepareForCommit( R record )
        {
            actual.prepareForCommit( record );
        }

        @Override
        public <EXCEPTION extends Exception> void scanAllRecords( Visitor<R,EXCEPTION> visitor ) throws EXCEPTION
        {
            actual.scanAllRecords( visitor );
        }

        @Override
        public void freeId( long id )
        {
            actual.freeId( id );
        }
    }

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

        protected <R extends AbstractBaseRecord> R getRecord( RecordStore<R> store, long id, R into )
        {
            store.getRecord( id, into, RecordLoad.FORCE );
            return into;
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
            ResourceIterable<R> iterable = Scanner.scan( store, true, filters );
            try ( ResourceIterator<R> scan = iterable.iterator() )
            {
                while ( scan.hasNext() )
                {
                    R record = scan.next();
                    if ( shouldStop )
                    {
                        break;
                    }

                    store.accept( this, record );
                    progressListener.set( record.getId() );
                }
                progressListener.done();
            }
        }
    }

    /**
     * Utility methods for reading records. These are not on the interface itself since it should be
     * an explicit choice when to create the record instances passed into it.
     * Also for mocking purposes it's less confusing and error prone having only a single method.
     */
    static <R extends AbstractBaseRecord> R getRecord( RecordStore<R> store, long id, RecordLoad mode )
    {
        R record = store.newRecord();
        store.getRecord( id, record, mode );
        return record;
    }

    /**
     * Utility methods for reading records. These are not on the interface itself since it should be
     * an explicit choice when to create the record instances passed into it.
     * Also for mocking purposes it's less confusing and error prone having only a single method.
     */
    static <R extends AbstractBaseRecord> R getRecord( RecordStore<R> store, long id )
    {
        return getRecord( store, id, RecordLoad.NORMAL );
    }
}

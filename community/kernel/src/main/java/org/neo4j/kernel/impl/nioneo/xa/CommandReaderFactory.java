/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.nioneo.xa;

import java.util.Collection;

import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyBlock;
import org.neo4j.kernel.impl.nioneo.store.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.xa.command.PhysicalLogNeoCommandReaderV0_19;
import org.neo4j.kernel.impl.nioneo.xa.command.PhysicalLogNeoCommandReaderV0_20;
import org.neo4j.kernel.impl.nioneo.xa.command.PhysicalLogNeoCommandReaderV1;

import static org.neo4j.kernel.impl.transaction.xaframework.log.entry.LogEntryVersions.LEGACY_LOG_ENTRY_VERSION;
import static org.neo4j.kernel.impl.transaction.xaframework.log.entry.LogEntryVersions.LOG_ENTRY_VERSION_2_1;
import static org.neo4j.kernel.impl.transaction.xaframework.log.entry.LogEntryVersions.LOG_ENTRY_VERSION_2_2;
import static org.neo4j.kernel.impl.transaction.xaframework.log.entry.LogVersions.LOG_VERSION_1_9;
import static org.neo4j.kernel.impl.transaction.xaframework.log.entry.LogVersions.LOG_VERSION_2_0;
import static org.neo4j.kernel.impl.transaction.xaframework.log.entry.LogVersions.LOG_VERSION_2_1;
import static org.neo4j.kernel.impl.transaction.xaframework.log.entry.LogVersions.LOG_VERSION_2_2;

public abstract class CommandReaderFactory
{
    public abstract CommandReader newInstance( byte logFormatVersion, byte logEntryVersion );

    public static class Default extends CommandReaderFactory
    {
        // Remember the latest log formant and log entry versions of the last reader returned.
        // Don't use a map because of the overhead of it.
        // Typically lots of log entries of the same version comes together.
        private byte lastFormatVersion;
        private byte lastEntryVersion;
        private CommandReader lastReader;

        @Override
        public CommandReader newInstance( byte logFormatVersion, byte logEntryVersion )
        {
            if ( logFormatVersion == lastFormatVersion && logEntryVersion == lastEntryVersion && lastReader != null )
            {
                return lastReader;
            }

            lastFormatVersion = logFormatVersion;
            lastEntryVersion = logEntryVersion;
            return (lastReader = figureOutCorrectReader( logFormatVersion, logEntryVersion ));
        }

        private CommandReader figureOutCorrectReader( byte logFormatVersion, byte logEntryVersion )
        {
            switch ( logEntryVersion )
            {
                // These are not thread safe, so if they are to be cached it has to be done in an object pool
                case LEGACY_LOG_ENTRY_VERSION:
                    switch ( logFormatVersion )
                    {
                        case LOG_VERSION_2_0:
                            return new PhysicalLogNeoCommandReaderV0_20();
                        case LOG_VERSION_1_9:
                            return new PhysicalLogNeoCommandReaderV0_19();
                    }
                case LOG_ENTRY_VERSION_2_1:
                case LOG_ENTRY_VERSION_2_2:
                    switch ( logFormatVersion )
                    {
                        case LOG_VERSION_2_1:
                        case LOG_VERSION_2_2:
                            return new PhysicalLogNeoCommandReaderV1();
                    }
            }
            throw new IllegalArgumentException( "Unknown log format version (" + logFormatVersion + ") and " +
                    "log entry version (" + logEntryVersion + ")" );

        }
    }

    public interface DynamicRecordAdder<T>
    {
        void add( T target, DynamicRecord record );
    }

    public static final DynamicRecordAdder<PropertyBlock> PROPERTY_BLOCK_DYNAMIC_RECORD_ADDER =
            new DynamicRecordAdder<PropertyBlock>()
    {
        @Override
        public void add( PropertyBlock target, DynamicRecord record )
        {
            record.setCreated();
            target.addValueRecord( record );
        }
    };

    public static final DynamicRecordAdder<Collection<DynamicRecord>> COLLECTION_DYNAMIC_RECORD_ADDER =
            new DynamicRecordAdder<Collection<DynamicRecord>>()
    {
        @Override
        public void add( Collection<DynamicRecord> target, DynamicRecord record )
        {
            target.add( record );
        }
    };

    public static final DynamicRecordAdder<PropertyRecord> PROPERTY_DELETED_DYNAMIC_RECORD_ADDER =
            new DynamicRecordAdder<PropertyRecord>()
    {
        @Override
        public void add( PropertyRecord target, DynamicRecord record )
        {
            assert !record.inUse() : record + " is kinda weird";
            target.addDeletedRecord( record );
        }
    };

    public static final DynamicRecordAdder<PropertyKeyTokenRecord> PROPERTY_INDEX_DYNAMIC_RECORD_ADDER =
            new DynamicRecordAdder<PropertyKeyTokenRecord>()
    {
        @Override
        public void add( PropertyKeyTokenRecord target, DynamicRecord record )
        {
            target.addNameRecord( record );
        }
    };
}

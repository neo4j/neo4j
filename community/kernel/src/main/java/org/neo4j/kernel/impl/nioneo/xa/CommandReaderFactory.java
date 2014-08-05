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
import org.neo4j.kernel.impl.nioneo.xa.command.PhysicalLogNeoCommandReaderV0;
import org.neo4j.kernel.impl.nioneo.xa.command.PhysicalLogNeoCommandReaderV1;

public abstract class CommandReaderFactory
{
    public abstract CommandReader newInstance( byte logEntryVersion );

    public static class Default extends CommandReaderFactory
    {
        // Remember the last version of the last reader returned. Don't use a map because of it the overhead
        // of it. Typically lots of log entries of the same version comes together.
        private byte lastVersion;
        private CommandReader lastReader;

        public Default()
        {
        }

        @Override
        public CommandReader newInstance( byte logEntryVersion )
        {
            if ( logEntryVersion == lastVersion && lastReader != null )
            {
                return lastReader;
            }

            lastVersion = logEntryVersion;
            return (lastReader = figureOutCorrectReader( logEntryVersion ));
        }

        private CommandReader figureOutCorrectReader( byte logEntryVersion )
        {
            switch ( logEntryVersion )
            {
                // These are not thread safe, so if they are to be cached it has to be done in an object pool
                case 0:
                    return new PhysicalLogNeoCommandReaderV0();
                case -1:
                    return new PhysicalLogNeoCommandReaderV1();
                default:
                    throw new IllegalArgumentException( "Unknown log entry version " + logEntryVersion );
            }
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

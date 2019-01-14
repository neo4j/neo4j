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
package org.neo4j.consistency.statistics;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;

import static java.lang.String.format;

/**
 * Keeps access statistics about a store, i.e. identifying
 * {@link RecordStore#getRecord(long, AbstractBaseRecord, RecordLoad)} patterns and how random the access is.
 */
public class AccessStatistics
{
    private final Map<RecordStore<? extends AbstractBaseRecord>,AccessStats> stats = new HashMap<>();

    public void register( RecordStore<? extends AbstractBaseRecord> store, AccessStats accessStats )
    {
        assert !stats.containsKey( store );
        stats.put( store, accessStats );
    }

    public String getAccessStatSummary()
    {
        String msg = "";
        for ( AccessStats accessStats : stats.values() )
        {
            String accessStat = accessStats.toString();
            if ( accessStat.length() != 0 )
            {
                msg += format( accessStat + "%n" );
            }
        }
        return msg;
    }

    public void reset()
    {
        for ( AccessStats accessStats : stats.values() )
        {
            accessStats.reset();
        }
    }

    public static class AccessStats
    {
        private long reads;
        private long writes;
        private long inUse;
        private long randomReads;
        private long randomWrites;
        private int proximityValue;
        private final String storeType;
        private long prevReadId;
        private long prevWriteId;

        public AccessStats( String type, int proximity )
        {
            this.storeType = type;
            this.proximityValue = proximity;
        }

        @Override
        public String toString()
        {
            if ( reads == 0 && writes == 0 && randomReads == 0 )
            {
                return "";
            }
            StringBuilder buf = new StringBuilder( storeType );
            appendStat( buf, "InUse", inUse );
            appendStat( buf, "Reads", reads );
            appendStat( buf, "Random Reads", randomReads );
            appendStat( buf, "Writes", writes );
            appendStat( buf, "Random Writes", randomWrites );
            int scatterIndex = 0;
            if ( randomReads > 0 )
            {
                long scatterReads = reads == 0 ? randomReads : reads;
                scatterIndex = (int) ((randomReads * 100) / scatterReads);
            }
            appendStat( buf, "ScatterIndex", scatterIndex );

            // TODO enable this comment again when we have an official property reorganization tool,
            // but keep here as a reminder to do so
//          if ( scatterIndex > 0.5 )
//          {
//              buf.append( format( "%n *** Property Store reorgization is recommended for optimal performance ***" ) );
//          }

            return buf.toString();
        }

        private void appendStat( StringBuilder target, String name, long stat )
        {
            if ( stat > 0 )
            {
                target.append( format( "%n  %s: %d", name, stat ) );
            }
        }

        public void reset()
        {
            this.reads = 0;
            this.writes = 0;
            this.randomReads = 0;
            this.randomReads = 0;
            this.randomWrites = 0;
            this.inUse = 0;
        }

        public void upRead( long id )
        {
            if ( prevReadId != id )
            {
                reads++;
                incrementRandomReads( id, prevReadId );
                prevReadId = id;
            }
        }

        private boolean notCloseBy( long id1, long id2 )
        {
            return id1 >= 0 && id2 >= 0 && Math.abs( id2 - id1 ) >= this.proximityValue;
        }

        public void upWrite( long id )
        {
            if ( prevWriteId != id )
            {
                writes++;
                if ( id > 0 && notCloseBy( id, prevWriteId ) )
                {
                    randomWrites++;
                }
                prevWriteId = id;
            }
        }

        public synchronized void incrementRandomReads( long id1, long id2 )
        {
            if ( notCloseBy( id1, id2 ) )
            {
                randomReads++;
            }
        }

        public synchronized void upInUse()
        {
            inUse++;
        }
    }
}

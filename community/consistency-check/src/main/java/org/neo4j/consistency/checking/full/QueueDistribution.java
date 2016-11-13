/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.consistency.checking.full;

import org.neo4j.consistency.checking.full.RecordDistributor.RecordConsumer;
import org.neo4j.helpers.Exceptions;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;

/**
 * Factory for creating {@link QueueDistribution}. Typically the distribution type is decided higher up
 * in the call stack and the actual {@link QueueDistributor} is instantiated when more data is available
 * deeper down in the call stack.
 */
public interface QueueDistribution
{
    <RECORD> QueueDistributor<RECORD> distributor( long recordsPerCpu, int numberOfThreads );

    /**
     * Distributes records into {@link RecordConsumer}.
     */
    interface QueueDistributor<RECORD>
    {
        void distribute( RECORD record, RecordConsumer<RECORD> consumer ) throws InterruptedException;
    }

    /**
     * Distributes records round-robin style to all queues.
     */
    QueueDistribution ROUND_ROBIN = new QueueDistribution()
    {
        @Override
        public <RECORD> QueueDistributor<RECORD> distributor( long recordsPerCpu, int numberOfThreads )
        {
            return new RoundRobinQueueDistributor<>( numberOfThreads );
        }
    };

    /**
     * Distributes {@link RelationshipRecord} depending on the start/end node ids.
     */
    QueueDistribution RELATIONSHIPS = new QueueDistribution()
    {
        @Override
        public QueueDistributor<RelationshipRecord> distributor( long recordsPerCpu, int numberOfThreads )
        {
            return new RelationshipNodesQueueDistributor( recordsPerCpu );
        }
    };

    class RoundRobinQueueDistributor<RECORD> implements QueueDistributor<RECORD>
    {
        private final int numberOfThreads;
        private int nextQIndex;

        RoundRobinQueueDistributor( int numberOfThreads )
        {
            this.numberOfThreads = numberOfThreads;
        }

        @Override
        public void distribute( RECORD record, RecordConsumer<RECORD> consumer ) throws InterruptedException
        {
            try
            {
                consumer.accept( record, nextQIndex );
            }
            finally
            {
                nextQIndex = (nextQIndex + 1) % numberOfThreads;
            }
        }
    }

    class RelationshipNodesQueueDistributor implements QueueDistributor<RelationshipRecord>
    {
        private final long recordsPerCpu;

        RelationshipNodesQueueDistributor( long recordsPerCpu )
        {
            this.recordsPerCpu = recordsPerCpu;
        }

        @Override
        public void distribute( RelationshipRecord relationship, RecordConsumer<RelationshipRecord> consumer )
                throws InterruptedException
        {
            int qIndex1 = (int) (relationship.getFirstNode() / recordsPerCpu);
            int qIndex2 = (int) (relationship.getSecondNode() / recordsPerCpu);
            try
            {
                consumer.accept( relationship, qIndex1 );
                if ( qIndex1 != qIndex2 )
                {
                    consumer.accept( relationship, qIndex2 );
                }
            }
            catch ( ArrayIndexOutOfBoundsException e )
            {
                throw Exceptions.withMessage( e, e.getMessage() + ", recordsPerCPU:" + recordsPerCpu +
                        ", relationship:" + relationship );
            }
        }
    }
}

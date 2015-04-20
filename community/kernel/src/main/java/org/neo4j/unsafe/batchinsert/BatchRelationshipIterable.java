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
package org.neo4j.unsafe.batchinsert;

import java.util.Iterator;

import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.kernel.impl.store.InvalidRecordException;
import org.neo4j.kernel.impl.store.NeoStore;
import org.neo4j.kernel.impl.store.RelationshipGroupStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;

public class BatchRelationshipIterable implements Iterable<RelationshipRecord>
{
    private final NeoStore neoStore;
    private final RelationshipStore relStore;
    private final RelationshipGroupStore groupStore;
    private final long nodeId;

    public BatchRelationshipIterable( NeoStore neoStore, long nodeId )
    {
        this.neoStore = neoStore;
        this.relStore = neoStore.getRelationshipStore();
        this.groupStore = neoStore.getRelationshipGroupStore();
        this.nodeId = nodeId;
    }

    @Override
    public Iterator<RelationshipRecord> iterator()
    {
        NodeRecord nodeRecord = neoStore.getNodeStore().getRecord( nodeId );
        if ( nodeRecord.isDense() )
        {
            return new DenseIterator( nodeRecord );
        }
        return new SparseIterator( nodeRecord );
    }

    public long nextRelationship( long relId, NodeRecord nodeRecord )
    {
        RelationshipRecord relRecord = relStore.getRecord( relId );
        long firstNode = relRecord.getFirstNode();
        long secondNode = relRecord.getSecondNode();
        long nextRel;
        if ( firstNode == nodeId )
        {
            nextRel = relRecord.getFirstNextRel();
        }
        else if ( secondNode == nodeId )
        {
            nextRel = relRecord.getSecondNextRel();
        }
        else
        {
            throw new InvalidRecordException( "Node[" + nodeId +
                    "] not part of firstNode[" + firstNode +
                    "] or secondNode[" + secondNode + "]" );
        }
        return nextRel;
    }

    public class SparseIterator extends PrefetchingIterator<RelationshipRecord>
    {
        private final NodeRecord nodeRecord;
        private long nextRelId;

        public SparseIterator( NodeRecord nodeRecord )
        {
            this.nodeRecord = nodeRecord;
            this.nextRelId = nodeRecord.getNextRel();
        }

        @Override
        protected RelationshipRecord fetchNextOrNull()
        {
            if ( nextRelId == Record.NO_NEXT_RELATIONSHIP.intValue() )
            {
                return null;
            }

            try
            {
                return relStore.getRecord( nextRelId );
            }
            finally
            {
                nextRelId = nextRelationship( nextRelId, nodeRecord );
            }
        }
    }

    public class DenseIterator extends PrefetchingIterator<RelationshipRecord>
    {
        private final NodeRecord nodeRecord;
        private RelationshipGroupRecord groupRecord;
        private int groupChainIndex;
        private long nextRelId;

        public DenseIterator( NodeRecord nodeRecord )
        {
            this.nodeRecord = nodeRecord;
            this.groupRecord = groupStore.getRecord( nodeRecord.getNextRel() );
            this.nextRelId = nextChainStart();
        }

        private long nextChainStart()
        {
            while ( groupRecord != null )
            {
                // Go to the next chain within the group
                while ( groupChainIndex < GroupChain.values().length )
                {
                    long chainStart = GroupChain.values()[groupChainIndex++].chainStart( groupRecord );
                    if ( chainStart != Record.NO_NEXT_RELATIONSHIP.intValue() )
                    {
                        return chainStart;
                    }
                }

                // Go to the next group
                groupRecord = groupRecord.getNext() != Record.NO_NEXT_RELATIONSHIP.intValue() ?
                        groupStore.getRecord( groupRecord.getNext() ) : null;
                groupChainIndex = 0;
            }
            return Record.NO_NEXT_RELATIONSHIP.intValue();
        }

        @Override
        protected RelationshipRecord fetchNextOrNull()
        {
            if ( nextRelId == Record.NO_NEXT_RELATIONSHIP.intValue() )
            {
                return null;
            }

            try
            {
                return relStore.getRecord( nextRelId );
            }
            finally
            {
                nextRelId = nextRelationship( nextRelId, nodeRecord );
                if ( nextRelId == Record.NO_NEXT_RELATIONSHIP.intValue() )
                {   // End of chain, try the next chain
                    nextRelId = nextChainStart();
                    // Potentially end of all chains here, and that's fine
                }
            }
        }
    }

    private static enum GroupChain
    {
        OUT
        {
            @Override
            long chainStart( RelationshipGroupRecord groupRecord )
            {
                return groupRecord.getFirstOut();
            }
        },
        IN
        {
            @Override
            long chainStart( RelationshipGroupRecord groupRecord )
            {
                return groupRecord.getFirstIn();
            }
        },
        LOOP
        {
            @Override
            long chainStart( RelationshipGroupRecord groupRecord )
            {
                return groupRecord.getFirstLoop();
            }
        };

        abstract long chainStart( RelationshipGroupRecord groupRecord );
    }
}

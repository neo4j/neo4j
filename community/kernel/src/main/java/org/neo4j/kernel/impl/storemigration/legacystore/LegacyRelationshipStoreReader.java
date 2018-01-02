/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.storemigration.legacystore;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

import org.neo4j.kernel.impl.store.record.RelationshipRecord;

public interface LegacyRelationshipStoreReader extends Closeable
{
    long getMaxId();

    int getRecordSize();

    Iterator<RelationshipRecord> iterator( long approximateStartId ) throws IOException;

    public static class ReusableRelationship
    {
        private long recordId;
        private boolean inUse;
        private long firstNode;
        private long secondNode;
        private int type;
        private long firstPrevRel;
        private long firstNextRel;
        private long secondNextRel;
        private long secondPrevRel;
        private long nextProp;

        private RelationshipRecord record;

        public void reset(long id, boolean inUse, long firstNode, long secondNode, int type, long firstPrevRel,
                          long firstNextRel, long secondNextRel, long secondPrevRel, long nextProp)
        {
            this.record = null;
            this.recordId = id;
            this.inUse = inUse;
            this.firstNode = firstNode;
            this.secondNode = secondNode;
            this.type = type;
            this.firstPrevRel = firstPrevRel;
            this.firstNextRel = firstNextRel;
            this.secondNextRel = secondNextRel;
            this.secondPrevRel = secondPrevRel;
            this.nextProp = nextProp;
        }

        public boolean inUse()
        {
            return inUse;
        }

        public long getFirstNode()
        {
            return firstNode;
        }

        public long getFirstNextRel()
        {
            return firstNextRel;
        }

        public long getSecondNode()
        {
            return secondNode;
        }

        public long getFirstPrevRel()
        {
            return firstPrevRel;
        }

        public long getSecondPrevRel()
        {
            return secondPrevRel;
        }

        public long getSecondNextRel()
        {
            return secondNextRel;
        }

        public long id()
        {
            return recordId;
        }

        public RelationshipRecord createRecord()
        {
            if( record == null)
            {
                record = new RelationshipRecord( recordId, firstNode, secondNode, type );
                record.setInUse( inUse );
                record.setFirstPrevRel( firstPrevRel );
                record.setFirstNextRel( firstNextRel );
                record.setSecondPrevRel( secondPrevRel );
                record.setSecondNextRel( secondNextRel );
                record.setNextProp( nextProp );

            }
            return record;
        }
    }
}

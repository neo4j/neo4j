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
package org.neo4j.kernel.impl.storemigration;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongObjectMap;
import org.neo4j.collection.primitive.PrimitiveLongObjectVisitor;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;

/**
 * Allows incrementally building up a relationship chain, and allows telling when the chain is complete.
 */
public class RelChainBuilder
{
    private final long nodeId;

    private PrimitiveLongObjectMap<RelationshipRecord> records = Primitive.longObjectMap();

    private int missing = 0;

    public RelChainBuilder( long nodeId )
    {
        this.nodeId = nodeId;
    }

    public void append( RelationshipRecord record, long prevRel, long nextRel )
    {
        if( records.containsKey( prevRel ))
        {
            missing--;
        }
        else if( !(prevRel == Record.NO_PREV_RELATIONSHIP.intValue()) )
        {
            missing++;
        }

        if( records.containsKey( nextRel ))
        {
            missing--;
        }
        else if( !(nextRel == Record.NO_NEXT_RELATIONSHIP.intValue()) )
        {
            missing++;
        }

        records.put( record.getId(), record );
    }

    public boolean isComplete()
    {
        return missing == 0;
    }

    public int size()
    {
        return records.size();
    }

    public long nodeId()
    {
        return nodeId;
    }

    public String toString()
    {
        return "RelChainBuilder{" +
                "nodeId=" + nodeId +
                ", records=" + records +
                ", missing=" + missing +
                '}';
    }

    public void accept( final Visitor<RelationshipRecord, RuntimeException> visitor )
    {
        records.visitEntries( new PrimitiveLongObjectVisitor<RelationshipRecord>()
        {
            @Override
            public void visited( long key, RelationshipRecord relationshipRecord )
            {
                visitor.visit( relationshipRecord );
            }
        });
    }
}

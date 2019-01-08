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
package org.neo4j.kernel.impl.api.store;

import org.neo4j.kernel.impl.api.RelationshipVisitor;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;

public class AllRelationshipIterator extends HighIdAwareIterator<RelationshipStore>
    implements RelationshipIterator
{
    private final RelationshipRecord record;

    private long currentId;

    AllRelationshipIterator( RelationshipStore store )
    {
        super( store );
        this.record = store.newRecord();
    }

    @Override
    public <EXCEPTION extends Exception> boolean relationshipVisit( long relationshipId,
            RelationshipVisitor<EXCEPTION> visitor ) throws EXCEPTION
    {
        visitor.visit( relationshipId, record.getType(), record.getFirstNode(), record.getSecondNode() );
        return false;
    }

    @Override
    protected boolean doFetchNext( long highId )
    {
        while ( currentId <= highId )
        {
            try
            {
                store.getRecord( currentId, record, RecordLoad.CHECK );
                if ( record.inUse() )
                {
                    return next( record.getId() );
                }
            }
            finally
            {
                currentId++;
            }
        }
        return false;
    }
}

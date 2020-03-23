/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.commandline.dbms.storeutil;

import java.io.IOException;

import org.neo4j.internal.batchimport.input.Group;
import org.neo4j.internal.batchimport.input.InputEntityVisitor;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.TokenHolder;

class LenientRelationshipReader extends LenientStoreInputChunk
{
    private final RelationshipStore relationshipStore;
    private final RelationshipRecord record;
    private final StoreCopyFilter.TokenLookup tokenLookup;

    LenientRelationshipReader( StoreCopyStats stats, RelationshipStore relationshipStore, PropertyStore propertyStore, TokenHolders tokenHolders,
            StoreCopyFilter storeCopyFilter )
    {
        super( stats, propertyStore, tokenHolders, relationshipStore.openPageCursorForReading( 0 ), storeCopyFilter );
        this.relationshipStore = relationshipStore;
        this.record = relationshipStore.newRecord();
        TokenHolder tokenHolder = tokenHolders.relationshipTypeTokens();
        tokenLookup = id -> tokenHolder.getTokenById( id ).name();
    }

    @Override
    void readAndVisit( long id, InputEntityVisitor visitor ) throws IOException
    {
        relationshipStore.getRecordByCursor( id, record, RecordLoad.NORMAL, cursor );
        if ( record.inUse() )
        {
            relationshipStore.ensureHeavy( record );
            int relType = record.getType();
            String relName = storeCopyFilter.filterRelationship( relType, tokenLookup );
            if ( relName != null )
            {
                visitor.type( relName );
                visitor.startId( record.getFirstNode(), Group.GLOBAL );
                visitor.endId( record.getSecondNode(), Group.GLOBAL );
                visitPropertyChainNoThrow( visitor, record );
                visitor.endOfEntity();
            }
            else
            {
                stats.removed.increment();
            }
        }
        else
        {
            stats.unused.increment();
        }
    }

    @Override
    String recordType()
    {
        return "Relationship";
    }
}

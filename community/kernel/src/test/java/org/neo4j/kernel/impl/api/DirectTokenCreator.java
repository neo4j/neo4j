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
package org.neo4j.kernel.impl.api;

import org.neo4j.kernel.impl.core.TokenCreator;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeTokenRecord;
import org.neo4j.kernel.impl.nioneo.xa.RecordAccessSet;
import org.neo4j.unsafe.batchinsert.DirectRecordAccessSet;

/**
 * Skips the transaction record state and goes and creates a relationship type directly in a {@link NeoStore}.
 */
public class DirectTokenCreator implements TokenCreator
{
    private final NeoStore neoStore;

    public DirectTokenCreator( NeoStore neoStore )
    {
        this.neoStore = neoStore;
    }

    @Override
    public int getOrCreate( String name )
    {
        RecordAccessSet recordAccess = new DirectRecordAccessSet( neoStore );
        int id = (int) neoStore.getRelationshipTypeTokenStore().nextId();
        org.neo4j.kernel.impl.nioneo.xa.TokenCreator<RelationshipTypeTokenRecord> creator =
                new org.neo4j.kernel.impl.nioneo.xa.TokenCreator<>( neoStore.getRelationshipTypeTokenStore() );
        creator.createToken( name, id, recordAccess.getRelationshipTypeTokenChanges() );
        recordAccess.close();
        return id;
    }
}

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
package org.neo4j.internal.recordstorage;

import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.eclipse.collections.impl.factory.primitive.LongLists;

import java.util.BitSet;
import java.util.concurrent.ExecutionException;

import org.neo4j.internal.id.IdGenerator;
import org.neo4j.internal.id.IdGenerator.CommitMarker;
import org.neo4j.util.concurrent.AsyncApply;
import org.neo4j.util.concurrent.WorkSync;

class ChangedIds
{
    private final MutableLongList ids = LongLists.mutable.empty();
    private final BitSet operation = new BitSet(); // set=used, cleared=unused
    private AsyncApply asyncApply;

    void addUsedId( long id )
    {
        operation.set( ids.size() );
        ids.add( id );
    }

    void addUnusedId( long id )
    {
        ids.add( id );
    }

    void accept( CommitMarker visitor )
    {
        ids.forEachWithIndex( ( id, index ) ->
        {
            boolean used = operation.get( index );
            if ( used )
            {
                visitor.markUsed( id );
            }
            else
            {
                visitor.markDeleted( id );
            }
        } );
    }

    void applyAsync( WorkSync<IdGenerator,IdGeneratorUpdateWork> workSync )
    {
        asyncApply = workSync.applyAsync( new IdGeneratorUpdateWork( this ) );
    }

    void awaitApply() throws ExecutionException
    {
        asyncApply.await();
    }
}

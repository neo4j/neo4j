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
package org.neo4j.kernel.impl.api.index;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.neo4j.helpers.collection.Pair;
import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.impl.store.MultipleUnderlyingStorageExceptions;
import org.neo4j.kernel.impl.store.UnderlyingStorageException;

/**
 * Holds currently open index updaters that are created dynamically when requested for any existing index in
 * the given indexMap.
 *
 * Also provides a close method for closing and removing any remaining open index updaters.
 *
 * All updaters retrieved from this map must be either closed manually or handle duplicate calls to close
 * or must all be closed indirectly by calling close on this updater map.
 */
class IndexUpdaterMap implements AutoCloseable, Iterable<IndexUpdater>
{
    private final IndexUpdateMode indexUpdateMode;
    private final IndexMap indexMap;
    private final Map<SchemaDescriptor,IndexUpdater> updaterMap;

    IndexUpdaterMap( IndexMap indexMap, IndexUpdateMode indexUpdateMode )
    {
        this.indexUpdateMode = indexUpdateMode;
        this.indexMap = indexMap;
        this.updaterMap = new HashMap<>();
    }

    IndexUpdater getUpdater( SchemaDescriptor descriptor )
    {
        IndexUpdater updater = updaterMap.get( descriptor );
        if ( null == updater )
        {
            IndexProxy indexProxy = indexMap.getIndexProxy( descriptor );
            if ( null != indexProxy )
            {
                updater = indexProxy.newUpdater( indexUpdateMode );
                updaterMap.put( descriptor, updater );
            }
        }
        return updater;
    }

    @Override
    public void close() throws UnderlyingStorageException
    {
        Set<Pair<SchemaDescriptor,UnderlyingStorageException>> exceptions = null;

        for ( Map.Entry<SchemaDescriptor,IndexUpdater> updaterEntry : updaterMap.entrySet() )
        {
            IndexUpdater updater = updaterEntry.getValue();
            try
            {
                updater.close();
            }
            catch ( IOException | IndexEntryConflictException e )
            {
                if ( null == exceptions )
                {
                    exceptions = new HashSet<>();
                }
                exceptions.add( Pair.of( updaterEntry.getKey(),
                        new UnderlyingStorageException( e ) ) );
            }
        }

        clear();

        if ( null != exceptions )
        {
            throw new MultipleUnderlyingStorageExceptions( exceptions );
        }
    }

    public void clear()
    {
        updaterMap.clear();
    }

    public boolean isEmpty()
    {
        return updaterMap.isEmpty();
    }

    public int size()
    {
        return updaterMap.size();
    }

    @Override
    public Iterator<IndexUpdater> iterator()
    {
        return new PrefetchingIterator<IndexUpdater>()
        {
            private Iterator<SchemaDescriptor> descriptors = indexMap.descriptors();
            @Override
            protected IndexUpdater fetchNextOrNull()
            {
                if ( descriptors.hasNext() )
                {
                    return getUpdater( descriptors.next() );
                }
                return null;
            }
        };
    }
}

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
package org.neo4j.kernel.impl.api.index;

import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.index.IndexDescriptor;

public class IndexMapReference implements IndexMapSnapshotProvider
{
    private volatile IndexMap indexMap = new IndexMap();

    @Override
    public IndexMap indexMapSnapshot()
    {
        return indexMap.clone();
    }

    public IndexProxy getIndexProxy( long indexId ) throws IndexNotFoundKernelException
    {
        IndexProxy proxy = indexMap.getIndexProxy( indexId );
        if ( proxy == null )
        {
            throw new IndexNotFoundKernelException( "No index for index id " + indexId + " exists." );
        }
        return proxy;
    }

    public IndexProxy getIndexProxy( IndexDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        IndexProxy proxy = indexMap.getIndexProxy( descriptor );
        if ( proxy == null )
        {
            throw new IndexNotFoundKernelException( "No index for " + descriptor + " exists." );
        }
        return proxy;
    }

    public IndexProxy getOnlineIndexProxy( IndexDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        IndexProxy proxy = getIndexProxy( descriptor );
        switch ( proxy.getState() )
        {
            case ONLINE:
                return proxy;

            default:
                throw new IndexNotFoundKernelException( "Expected index on " + descriptor + " to be online.");
        }
    }

    public Iterable<IndexProxy> getAllIndexProxies()
    {
        return indexMap.getAllIndexProxies();
    }

    public void setIndexMap( IndexMap newIndexMap )
    {
        // ASSUMPTION: Only called at shutdown or during commit (single-threaded in each case)
        indexMap = newIndexMap;
    }

    public IndexProxy removeIndexProxy( long indexId )
    {
        // ASSUMPTION: Only called at shutdown or during commit (single-threaded in each case)
        IndexMap newIndexMap = indexMapSnapshot();
        IndexProxy indexProxy = newIndexMap.removeIndexProxy( indexId );
        setIndexMap( newIndexMap );
        return indexProxy;
    }

    public Iterable<IndexProxy> clear()
    {
        // ASSUMPTION: Only called at shutdown when there are no other calls to setIndexMap
        IndexMap oldIndexMap = indexMap;
        setIndexMap( new IndexMap() );
        return oldIndexMap.getAllIndexProxies();
    }

    public IndexUpdaterMap createIndexUpdaterMap( IndexUpdateMode mode )
    {
        return new IndexUpdaterMap( indexMap, mode );
    }
}

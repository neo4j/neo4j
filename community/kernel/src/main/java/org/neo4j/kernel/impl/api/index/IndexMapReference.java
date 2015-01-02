/**
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
package org.neo4j.kernel.impl.api.index;

public class IndexMapReference
{
    private volatile IndexMap indexMap = new IndexMap();

    public IndexMap getIndexMapCopy()
    {
        return indexMap.clone();
    }

    public IndexProxy getIndexProxy( long indexId )
    {
        return indexMap.getIndexProxy( indexId );
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
        IndexMap newIndexMap = getIndexMapCopy();
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

    public IndexUpdaterMap getIndexUpdaterMap( IndexUpdateMode mode )
    {
        return new IndexUpdaterMap( mode, indexMap );
    }
}

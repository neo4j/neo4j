/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.kernel.impl.nioneo.store;

import org.neo4j.kernel.impl.core.ReadOnlyDbException;

public class ReadOnlyIdGenerator implements IdGenerator
{
    private final String fileName;
    private final long highId;
    
    public ReadOnlyIdGenerator( String fileName, long highId )
    {
        this.fileName = fileName;
        this.highId = highId;
    }

    public long nextId()
    {
        throw new ReadOnlyDbException();
    }
    
    public IdRange nextIdBatch( int size )
    {
        throw new ReadOnlyDbException();
    }
    
    public void setHighId( long id )
    {
        throw new ReadOnlyDbException();
    }

    public long getHighId()
    {
        return highId;
    }

    public void freeId( long id )
    {
        throw new ReadOnlyDbException();
    }

    @Override
    public void close( boolean shutdown )
    {
    }

    public String getFileName()
    {
        return this.fileName;
    }

    public long getNumberOfIdsInUse()
    {
        return highId;
    }
    
    public long getDefragCount()
    {
        return 0;
    }
    
    @Override
    public void delete()
    {
        throw new ReadOnlyDbException();
    }
}
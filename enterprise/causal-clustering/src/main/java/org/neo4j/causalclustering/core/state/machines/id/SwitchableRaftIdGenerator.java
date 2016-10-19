/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.causalclustering.core.state.machines.id;

import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.kernel.impl.store.id.IdGenerator;
import org.neo4j.kernel.impl.store.id.IdRange;
import org.neo4j.logging.LogProvider;

class SwitchableRaftIdGenerator implements IdGenerator
{
    private final IdType idType;
    private final ReplicatedIdRangeAcquirer acquirer;
    private final LogProvider logProvider;
    private volatile IdGenerator delegate;

    SwitchableRaftIdGenerator( IdGenerator initialDelegate, IdType idType, ReplicatedIdRangeAcquirer acquirer, LogProvider
            logProvider )
    {
        delegate = initialDelegate;
        this.idType = idType;
        this.acquirer = acquirer;
        this.logProvider = logProvider;
    }

    void switchToRaft()
    {
        long highId = delegate.getHighId();
        delegate.close();
        delegate = new ReplicatedIdGenerator( idType, highId, acquirer, logProvider );
    }

    @Override
    public IdRange nextIdBatch( int size )
    {
        return delegate.nextIdBatch( size );
    }

    @Override
    public long nextId()
    {
        return delegate.nextId();
    }

    @Override
    public long getHighId()
    {
        return delegate.getHighId();
    }

    @Override
    public void setHighId( long id )
    {
        delegate.setHighId( id );
    }

    @Override
    public long getHighestPossibleIdInUse()
    {
        return delegate.getHighestPossibleIdInUse();
    }

    @Override
    public void freeId( long id )
    {
        delegate.freeId( id );
    }

    @Override
    public void close()
    {
        delegate.close();
    }

    @Override
    public long getNumberOfIdsInUse()
    {
        return delegate.getNumberOfIdsInUse();
    }

    @Override
    public long getDefragCount()
    {
        return delegate.getDefragCount();
    }

    @Override
    public void delete()
    {
        delegate.delete();
    }

}

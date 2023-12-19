/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.kernel.impl.enterprise.lock.forseti;

import java.time.Clock;

import org.neo4j.helpers.Service;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.ResourceTypes;
import org.neo4j.storageengine.api.lock.ResourceType;

@Service.Implementation( Locks.Factory.class )
public class ForsetiLocksFactory extends Locks.Factory
{
    public static final String KEY = "forseti";

    public ForsetiLocksFactory()
    {
        super( KEY );
    }

    @Override
    public Locks newInstance( Config config, Clock clock, ResourceType[] resourceTypes )
    {
        return new ForsetiLockManager( config, clock, ResourceTypes.values() );
    }
}

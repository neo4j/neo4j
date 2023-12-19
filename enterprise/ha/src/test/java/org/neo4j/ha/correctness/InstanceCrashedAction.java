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
package org.neo4j.ha.correctness;

import org.neo4j.helpers.collection.Iterables;

public class InstanceCrashedAction implements ClusterAction
{
    private final String instanceUri;

    public InstanceCrashedAction( String instanceUri )
    {
        this.instanceUri = instanceUri;
    }

    @Override
    public Iterable<ClusterAction> perform( ClusterState state ) throws Exception
    {
        state.instance( instanceUri ).crash();
        return Iterables.empty();
    }

    @Override
    public String toString()
    {
        return "[CRASH " + instanceUri + "]";
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        return instanceUri.equals( ((InstanceCrashedAction)o).instanceUri );
    }

    @Override
    public int hashCode()
    {
        return instanceUri.hashCode();
    }
}

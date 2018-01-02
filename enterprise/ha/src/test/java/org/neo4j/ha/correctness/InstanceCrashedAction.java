/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
        return "[CRASH "+instanceUri+"]";
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

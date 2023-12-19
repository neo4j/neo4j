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
package org.neo4j.cluster.protocol.election;

/**
 * Role that an instance can have in a cluster.
 */
public class ElectionRole
{
    private String name;

    public ElectionRole( String name )
    {
        this.name = name;
    }

    public String getName()
    {
        return name;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        ElectionRole that = (ElectionRole) o;

        return !(name != null ? !name.equals( that.name ) : that.name != null);

    }

    @Override
    public int hashCode()
    {
        return name != null ? name.hashCode() : 0;
    }
}

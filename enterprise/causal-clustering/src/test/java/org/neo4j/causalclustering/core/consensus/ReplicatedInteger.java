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
package org.neo4j.causalclustering.core.consensus;

import java.util.Objects;

import org.neo4j.causalclustering.core.replication.ReplicatedContent;

import static java.lang.String.format;

public class ReplicatedInteger implements ReplicatedContent
{
    private final Integer value;

    private ReplicatedInteger( Integer data )
    {
        Objects.requireNonNull( data );
        this.value = data;
    }

    public static ReplicatedInteger valueOf( Integer value )
    {
        return new ReplicatedInteger( value );
    }

    public int get()
    {
        return value;
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

        ReplicatedInteger that = (ReplicatedInteger) o;
        return value.equals( that.value );
    }

    @Override
    public int hashCode()
    {
        return value.hashCode();
    }

    @Override
    public String toString()
    {
        return format( "Integer(%d)", value );
    }
}

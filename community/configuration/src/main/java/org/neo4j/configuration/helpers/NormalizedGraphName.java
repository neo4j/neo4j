/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.configuration.helpers;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class NormalizedGraphName
{

    private String name;

    public NormalizedGraphName( String name )
    {
        requireNonNull( name, "Graph name should be not null." );
        this.name = name.toLowerCase();
    }

    public String name()
    {
        return name;
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
        NormalizedGraphName that = (NormalizedGraphName) o;
        return Objects.equals( name, that.name );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( name );
    }

    @Override
    public String toString()
    {
        return "NormalizedGraphName{ name='" + name + "'}";
    }
}

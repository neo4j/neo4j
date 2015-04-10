/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.driver.internal.value;

import org.neo4j.driver.Value;

public class NodeValue extends ValueAdapter
{
    private final org.neo4j.driver.Node adapted;

    public NodeValue( org.neo4j.driver.Node adapted )
    {
        this.adapted = adapted;
    }

    @Override
    public org.neo4j.driver.Node asNode()
    {
        return adapted;
    }

    @Override
    public boolean isNode()
    {
        return true;
    }

    @Override
    public long size()
    {
        int count = 0;
        for ( String ignore : adapted.propertyKeys() ) { count++; }
        return count;
    }

    @Override
    public Iterable<String> keys()
    {
        return adapted.propertyKeys();
    }

    @Override
    public Value get( String key )
    {
        return adapted.property( key );
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

        NodeValue values = (NodeValue) o;

        return !(adapted != null ? !adapted.equals( values.adapted ) : values.adapted != null);

    }

    @Override
    public int hashCode()
    {
        return adapted != null ? adapted.hashCode() : 0;
    }
}

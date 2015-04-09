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
package org.neo4j.driver.internal;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import org.neo4j.driver.Identity;
import org.neo4j.driver.Node;
import org.neo4j.driver.Value;

/**
 * {@link org.neo4j.driver.Node} implementation that directly contains labels and properties.
 */
public class SimpleNode extends SimpleEntity implements Node
{
    private final Collection<String> labels;

    public SimpleNode( String id )
    {
        this( id, Collections.<String>emptyList(), Collections.<String,Value>emptyMap() );
    }

    public SimpleNode( String id, Collection<String> labels, Map<String,Value> properties )
    {
        this( Identities.identity( id ), labels, properties );
    }

    public SimpleNode( Identity identity, Collection<String> labels, Map<String,Value> properties )
    {
        super( identity, properties );
        this.labels = labels;
    }

    @Override
    public Collection<String> labels()
    {
        return labels;
    }

    @Override
    public String toString()
    {
        return "Node{" + super.toString() + ", labels=" + labels + '}';
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
        if ( !super.equals( o ) )
        {
            return false;
        }

        SimpleNode that = (SimpleNode) o;

        return !(labels != null ? !labels.equals( that.labels ) : that.labels != null);

    }

    @Override
    public int hashCode()
    {
        int result = super.hashCode();
        result = 31 * result + (labels != null ? labels.hashCode() : 0);
        return result;
    }
}

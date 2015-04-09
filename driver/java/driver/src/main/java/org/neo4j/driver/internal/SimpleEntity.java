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
import java.util.Map;

import org.neo4j.driver.Entity;
import org.neo4j.driver.Identity;
import org.neo4j.driver.Value;

public abstract class SimpleEntity implements Entity
{
    private final Identity id;
    private final Map<String,Value> properties;

    public SimpleEntity( Identity id, Map<String,Value> properties )
    {
        this.id = id;
        this.properties = properties;
    }

    @Override
    public Identity identity()
    {
        return id;
    }

    @Override
    public Collection<String> propertyKeys()
    {
        return properties.keySet();
    }

    @Override
    public Value property( String key )
    {
        return properties.get( key );
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

        SimpleEntity that = (SimpleEntity) o;

        return id.equals( that.id );

    }

    @Override
    public int hashCode()
    {
        return id.hashCode();
    }

    @Override
    public String toString()
    {
        return "Entity{" +
               "id=" + id +
               ", properties=" + properties +
               '}';
    }
}

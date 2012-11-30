/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.performance.domain;

import org.codehaus.jackson.annotate.JsonProperty;

/**
 * Used to add some meta-data about what a metric actually measures, used by charts for labelling.
 * Perhaps can be used for more advanced stuff later on.
 */
public class Unit
{

    @JsonProperty private String key;

    private Unit() {}

    public Unit( String key )
    {
        this.key = key;
    }

    public Unit per(Unit other)
    {
        return new Unit( key + " / " + other.key );
    }

    public String asSuffix()
    {
        return key;
    }

    @Override
    public String toString()
    {
        return "Unit[" + key + "]";
    }

    @Override
    public boolean equals(Object other)
    {
        if(other instanceof Unit)
        {
            return key.equals( ((Unit)other).key );
        }
        else
        {
            return false;
        }
    }

}

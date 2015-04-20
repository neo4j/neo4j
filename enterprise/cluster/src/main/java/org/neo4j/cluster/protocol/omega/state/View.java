/*
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
package org.neo4j.cluster.protocol.omega.state;

public class View
{
    private final State state;
    private boolean expired;

    public View( State state )
    {
        this(state, true);
    }

    public View( State state, boolean expired )
    {
        this.state = state;
        this.expired = expired;
    }

    public State getState()
    {
        return state;
    }

    public boolean isExpired()
    {
        return expired;
    }

    public void setExpired( boolean expired )
    {
        this.expired = expired;
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( obj == null )
        {
            return false;
        }
        if ( !(obj instanceof View ) )
        {
            return false;
        }
        View other = (View) obj;
        return state.equals( other.state ) && expired == other.expired;
    }

    @Override
    public String toString()
    {
        return "View [state:"+state+", expired= "+expired+"]";
    }
}

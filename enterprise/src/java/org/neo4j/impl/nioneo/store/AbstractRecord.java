/*
 * Copyright (c) 2002-2009 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.impl.nioneo.store;

public abstract class AbstractRecord
{
    private boolean inUse = false;
    private final int id;
    private boolean created = false;

    AbstractRecord( int id )
    {
        this.id = id;
    }

    AbstractRecord( int id, boolean inUse )
    {
        this.id = id;
        this.inUse = inUse;
    }

    public int getId()
    {
        return id;
    }

    public boolean inUse()
    {
        return inUse;
    }

    public void setInUse( boolean inUse )
    {
        this.inUse = inUse;
    }

    public void setCreated()
    {
        this.created = true;
    }

    public boolean isCreated()
    {
        return created;
    }
}
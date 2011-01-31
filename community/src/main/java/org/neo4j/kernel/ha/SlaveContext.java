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
package org.neo4j.kernel.ha;

import java.util.Arrays;

import org.neo4j.helpers.Pair;

public final class SlaveContext
{
    private final int machineId;
    private final Pair<String, Long>[] lastAppliedTransactions;
    private final int eventIdentifier;
    private final int hashCode;

    public SlaveContext( int machineId, int eventIdentifier,
            Pair<String, Long>[] lastAppliedTransactions )
    {
        this.machineId = machineId;
        this.eventIdentifier = eventIdentifier;
        this.lastAppliedTransactions = lastAppliedTransactions;
        this.hashCode = 3*eventIdentifier*machineId;
    }

    public int machineId()
    {
        return machineId;
    }

    public Pair<String, Long>[] lastAppliedTransactions()
    {
        return lastAppliedTransactions;
    }
    
    public int getEventIdentifier()
    {
        return eventIdentifier;
    }

    @Override
    public String toString()
    {
        return "SlaveContext[ID:" + machineId + ", eventIdentifier:" + eventIdentifier + ", " +
                Arrays.asList( lastAppliedTransactions ) + "]";
    }
    
    @Override
    public boolean equals( Object obj )
    {
        if ( !( obj instanceof SlaveContext ) )
        {
            return false;
        }
        SlaveContext o = (SlaveContext) obj;
        return o.eventIdentifier == eventIdentifier && o.machineId == machineId;
    }
    
    @Override
    public int hashCode()
    {
        return this.hashCode;
    }
}

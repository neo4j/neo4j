/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.ha.cluster;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

import org.neo4j.backup.OnlineBackupKernelExtension;
import org.neo4j.cluster.member.paxos.MemberIsAvailable;
import org.neo4j.helpers.Function2;
import org.neo4j.helpers.collection.Iterables;

import static org.neo4j.kernel.ha.cluster.HighAvailabilityModeSwitcher.MASTER;
import static org.neo4j.kernel.ha.cluster.HighAvailabilityModeSwitcher.SLAVE;

/*
 * Filters existing events in a snapshot while adding new ones. Ensures that the snapshot is consistent in the
 * face of failures of instances in the cluster.
 */
public class HANewSnapshotFunction
        implements Function2<Iterable<MemberIsAvailable>, MemberIsAvailable, Iterable<MemberIsAvailable>>, Serializable
{
    @Override
    public Iterable<MemberIsAvailable> apply( Iterable<MemberIsAvailable> previousSnapshot,
            final MemberIsAvailable newMessage )
    {
        /*
         * If a master event is received, all events that set to slave that instance should be removed. The same
         * should happen to existing master events and backup events, no matter which instance they are for
         */
        if ( newMessage.getRole().equals( MASTER ) )
        {
            List<MemberIsAvailable> result = new LinkedList<>();
            for ( MemberIsAvailable existing : previousSnapshot )
            {
                if ( (isSlave( existing ) && sameIds( newMessage, existing ))
                        || isMaster( existing ) )
                {
                    continue;
                }
                result.add( existing );
            }
            result.add( newMessage );
            return result;
        }
        /*
         * If a slave event is received, all existing slave events for that instance should be removed. The same for
         * master and backup, which means remove all events for that instance.
         */
        else if ( newMessage.getRole().equals( SLAVE ) )
        {
            List<MemberIsAvailable> result = new LinkedList<>();
            for ( MemberIsAvailable existing : previousSnapshot )
            {
                if ( sameIds( newMessage, existing ) )
                {
                    continue;
                }
                result.add( existing );
            }
            result.add( newMessage );
            return result;
        }
        else if ( newMessage.getRole().equals( OnlineBackupKernelExtension.BACKUP ) )
        {
            List<MemberIsAvailable> result = new LinkedList<>();
            for ( MemberIsAvailable existing : previousSnapshot )
            {
                if ( existing.getRole().equals( OnlineBackupKernelExtension.BACKUP ) )
                {
                    continue;
                }
                result.add( existing );
            }
            result.add( newMessage );
            return result;
        }
        return Iterables.append( newMessage, previousSnapshot );
    }

    private boolean isMaster( MemberIsAvailable existing )
    {
        return existing.getRole().equals( MASTER );
    }

    private boolean isSlave( MemberIsAvailable existing )
    {
        return existing.getRole().equals( SLAVE );
    }

    private boolean sameIds( MemberIsAvailable newMessage, MemberIsAvailable existing )
    {
        return existing.getInstanceId().equals( newMessage.getInstanceId() );
    }
}

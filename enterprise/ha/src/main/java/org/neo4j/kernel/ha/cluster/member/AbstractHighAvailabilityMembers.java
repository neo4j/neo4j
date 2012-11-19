/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.kernel.ha.cluster.member;

import java.net.URI;

import org.neo4j.cluster.Binding;
import org.neo4j.cluster.BindingListener;
import org.neo4j.kernel.ha.cluster.HighAvailability;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberChangeEvent;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberListener;

/**
 * Basic foundation for listening to events going on in the cluster and
 * keep up to date information for when requested.
 * 
 * @author Mattias Persson
 */
public abstract class AbstractHighAvailabilityMembers
{
    private volatile URI myServerUri;
    private volatile URI lastKnownElectedMaster;

    protected AbstractHighAvailabilityMembers( Binding binding, HighAvailability highAvailability )
    {
        binding.addBindingListener( new LocalBindingListener() );
        highAvailability.addHighAvailabilityMemberListener( new LocalHighAvailabilityMemberListener() );
    }
    
    private class LocalBindingListener implements BindingListener
    {
        @Override
        public void listeningAt( URI me )
        {
            myServerUri = me;
        }
    }
    
    private class LocalHighAvailabilityMemberListener extends HighAvailabilityMemberListener.Adapter
    {
        @Override
        public void masterIsElected( HighAvailabilityMemberChangeEvent event )
        {
            URI electedMaster = event.getServerClusterUri();
            if ( changed( electedMaster ) )
            {
                newMasterElected();
                lastKnownElectedMaster = electedMaster;
            }
        }

        private boolean changed( URI electedMaster )
        {
            return lastKnownElectedMaster == null || !electedMaster.equals( lastKnownElectedMaster );
        }

        @Override
        public void slaveIsAvailable( HighAvailabilityMemberChangeEvent event )
        {
            AbstractHighAvailabilityMembers.this.slaveIsAvailable( event.getServerClusterUri(), event.getServerHaUri(), iAmMaster() );
        }

        @Override
        public void masterIsAvailable( HighAvailabilityMemberChangeEvent event )
        {
            AbstractHighAvailabilityMembers.this.masterIsAvailable( event.getServerClusterUri(), event.getServerHaUri(), iAmMaster() );
        }

        private boolean iAmMaster()
        {
            return lastKnownElectedMaster.equals( getMyServerUri() );
        }
    }

    protected URI getMyServerUri()
    {
        if ( myServerUri == null )
            throw new IllegalStateException( "My server URI not retreived yet" );
        return myServerUri;
    }

    protected void newMasterElected()
    {
    }

    protected void slaveIsAvailable( URI serverClusterUri, URI serverHaUri, boolean iAmMaster )
    {
    }

    protected void masterIsAvailable( URI serverClusterUri, URI serverHaUri, boolean iAmMaster )
    {
    }
}

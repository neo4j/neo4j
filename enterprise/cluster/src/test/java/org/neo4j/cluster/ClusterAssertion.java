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
package org.neo4j.cluster;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class ClusterAssertion
{
    protected Map<Integer, InstanceAssertion> in;
    protected Map<Integer, InstanceAssertion> out;

    private ClusterAssertion()
    {
    }

    public static ClusterAssertion basedOn( int[] serverIds )
    {
        Map<Integer, InstanceAssertion> out = new HashMap<Integer, InstanceAssertion>();

        for ( int i = 0; i < serverIds.length; i++ )
        {
            out.put( i + 1, new InstanceAssertion( serverIds[i], URI.create( "cluster://server" + serverIds[i] ) ) );
        }

        ClusterAssertion result = new ClusterAssertion();
        result.in = new HashMap<Integer, InstanceAssertion>(  );
        result.out = out;
        return result;
    }

    protected void copy( ClusterAssertion assertion )
    {
        this.in = assertion.in;
        this.out = assertion.out;
    }

    public ClusterAssertion joins( int... joiners )
    {
        final Map<Integer, InstanceAssertion> newIn = new HashMap<Integer, InstanceAssertion>( in );
        final Map<Integer, InstanceAssertion> newOut = new HashMap<Integer, InstanceAssertion>( out );
        for ( int joiner : joiners )
        {
            newIn.put( joiner, newOut.remove( joiner ) );
        }
        return new ClusterAssertion()
        {{
            this.in = newIn;
            this.out = newOut;
        }};
    }

    public ClusterAssertion failed( final int failed )
    {
        final Map<Integer, InstanceAssertion> newIn = new HashMap<Integer, InstanceAssertion>( in );
        final InstanceAssertion current = in.get( failed );

        InstanceAssertion failedInstance = new InstanceAssertion()
        {{
            copy( current );
            this.isFailed = true;
        }};

        newIn.put( failed, failedInstance );

        final ClusterAssertion realThis = this;
        return new ClusterAssertion()
        {{
            copy( realThis );
            this.in = newIn;
        }};
    }

    public ClusterAssertion elected( final int elected, final String atRole )
    {
        final Map<Integer, InstanceAssertion> newIn = new HashMap<Integer, InstanceAssertion>();
        for ( final Map.Entry<Integer, InstanceAssertion> instanceAssertionEntry : in.entrySet() )
        {
            final InstanceAssertion assertion = instanceAssertionEntry.getValue();
            if ( !instanceAssertionEntry.getValue().isFailed )
            {
                newIn.put( instanceAssertionEntry.getKey(), new InstanceAssertion()
                {{
                    copy( assertion );
                    this.roles.put( atRole, elected );
                }});
            }
            else
            {
                newIn.put( instanceAssertionEntry.getKey(), instanceAssertionEntry.getValue() );
            }
        }
        final ClusterAssertion realThis = this;
        return new ClusterAssertion()
        {{
                copy( realThis );
                this.in = newIn;
        }};
    }

    public InstanceAssertion[] snapshot()
    {
        InstanceAssertion[] result = new InstanceAssertion[in.size()+out.size()];
        for ( Map.Entry<Integer, InstanceAssertion> inEntry : in.entrySet() )
        {
            int key = inEntry.getKey() - 1;
            assert result[ key ] == null : "double entry for "+inEntry.getKey();
            result[ key ] = inEntry.getValue();
        }
        for ( Map.Entry<Integer, InstanceAssertion> outEntry : out.entrySet() )
        {
            int key = outEntry.getKey() - 1;
            assert result[ key ] == null : "double entry for " + outEntry.getKey();
            result[ key ] = outEntry.getValue();
        }
        return result;
    }

    public static class InstanceAssertion
    {
        int serverId;
        URI uri;
        boolean isFailed;
        Map<Integer, URI> reachableInstances = new HashMap<Integer, URI>();
        Map<Integer, URI> failedInstances = new HashMap<Integer, URI>();
        Map<String, Integer> roles = new HashMap<String, Integer>();

        public InstanceAssertion()
        {}

        public InstanceAssertion( int serverId, URI uri )
        {
            this.serverId = serverId;
            this.uri = uri;
        }

        protected void copy( InstanceAssertion instance )
        {
            this.serverId = instance.serverId;
            this.uri = instance.uri;
            this.isFailed = instance.isFailed;
            this.reachableInstances = new HashMap<Integer, URI>( instance.reachableInstances );
            this.failedInstances = new HashMap<Integer, URI>( instance.failedInstances );
            this.roles = new HashMap<String, Integer>( instance.roles );
        }
    }
}

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
package org.neo4j.kernel.ha.cluster.member;

import static java.util.Arrays.asList;

import java.net.URI;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.management.ClusterMemberInfo;

public class ClusterMemberMatcher extends BaseMatcher<Iterable<ClusterMemberInfo>>
{
    private boolean exactMatch;
    private ClusterMemberMatch[] expectedMembers;

    public ClusterMemberMatcher( boolean exactMatch, ClusterMemberMatch[] expected )
    {
        this.exactMatch = exactMatch;
        this.expectedMembers = expected;
    }

    @Override
    public void describeTo( Description description )
    {
        description.appendText( Arrays.toString( expectedMembers ) );
    }

    public static Matcher<ClusterMember> sameMemberAs( final ClusterMember clusterMember)
    {
        return new BaseMatcher<ClusterMember>()
        {
            @Override
            public boolean matches( Object instance )
            {
                if ( instance instanceof ClusterMember )
                {

                    ClusterMember member = ClusterMember.class.cast( instance );
                    if ( !member.getInstanceId().equals( clusterMember.getInstanceId() ) )
                    {
                        return false;
                    }

                    if ( member.isAlive() != clusterMember.isAlive() )
                    {
                        return false;
                    }

                    HashSet<URI> memberUris = new HashSet<>( Iterables.toList( member.getRoleURIs() ) );
                    HashSet<URI> clusterMemberUris = new HashSet<>( Iterables.toList( clusterMember.getRoleURIs() ) );
                    return memberUris.equals( clusterMemberUris );
                }
                else
                {
                    return false;
                }
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( "member should match " ).appendValue( clusterMember );
            }
        };
    }

    @Override
    public boolean matches( Object item )
    {
        if ( item instanceof Iterable )
        {
            @SuppressWarnings( "unchecked" )
            Iterable<ClusterMemberInfo> other = (Iterable<ClusterMemberInfo>) item;
            int foundCount = 0;
            for ( ClusterMemberMatch expectedMember : expectedMembers )
            {
                boolean found = false;
                for ( ClusterMemberInfo member : other )
                {
                    if ( expectedMember.match( member ) )
                    {
                        found = true;
                        foundCount++;
                        break;
                    }
                }
                if ( !found )
                {
                    return false;
                }
            }

            if ( exactMatch && foundCount != expectedMembers.length )
            {
                return false;
            }
        }
        else
        {
            return false;
        }
        return true;
    }
    
    public static ClusterMemberMatch member( URI member )
    {
        return new ClusterMemberMatch( member );
    }
    
    public static ClusterMemberMatcher containsMembers( ClusterMemberMatch... expected )
    {
        return new ClusterMemberMatcher( false, expected );
    }

    public static ClusterMemberMatcher containsOnlyMembers( ClusterMemberMatch... expected )
    {
        return new ClusterMemberMatcher( true, expected );
    }
    
    public static class ClusterMemberMatch
    {
        private URI member;
        private Boolean available;
        private Boolean alive;
        private String haRole;
        private Set<String> uris;

        ClusterMemberMatch( URI member )
        {
            this.member = member;
        }
        
        public ClusterMemberMatch available( boolean available )
        {
            this.available = available;
            return this;
        }
        
        public ClusterMemberMatch alive( boolean alive )
        {
            this.alive = alive;
            return this;
        }
        
        private boolean match( ClusterMemberInfo toMatch )
        {
            if ( !member.toString().equals( toMatch.getInstanceId() ) )
                return false;
            if ( available != null && toMatch.isAvailable() != available )
                return false;
            if ( alive != null && toMatch.isAlive() != alive )
                return false;
            if ( haRole != null && !haRole.equals( toMatch.getHaRole() ) )
                return false;
            if ( uris != null && !uris.equals( new HashSet<String>( asList( toMatch.getUris() ) ) ) )
                return false;
            return true;
        }
        
        @Override
        public String toString()
        {
            StringBuilder builder = new StringBuilder( "Member[" + member );
            if ( available != null )
            {
                builder.append( ", available:" ).append( available );
            }
            if ( alive != null )
            {
                builder.append( ", alive:").append( alive );
            }
            if ( haRole != null )
            {
                builder.append( ", haRole:").append( haRole );
            }
            if ( uris != null )
            {
                builder.append( ", uris:" ).append( uris );
            }
            return builder.append( "]" ).toString();
        }

        public ClusterMemberMatch haRole( String role )
        {
            this.haRole = role;
            return this;
        }
        
        public ClusterMemberMatch uris( URI... uris )
        {
            this.uris = new HashSet<>();
            for ( URI uri : uris )
            {
                this.uris.add( uri.toString() );
            }
            return this;
        }
    }
}

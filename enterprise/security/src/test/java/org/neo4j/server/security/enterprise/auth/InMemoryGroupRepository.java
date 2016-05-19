/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.server.security.enterprise.auth;

import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

import org.neo4j.kernel.api.security.exception.IllegalCredentialsException;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.server.security.auth.exception.ConcurrentModificationException;

/** A group repository implementation that just stores groups in memory */
public class InMemoryGroupRepository extends LifecycleAdapter implements GroupRepository
{
    private final ConcurrentHashMap<String, GroupRecord> groups = new ConcurrentHashMap<>();
    private final Map<String, SortedSet<String>> groupsByUsername = new ConcurrentHashMap<>();

    @Override
    public GroupRecord findByName( String name )
    {
        return groups.get( name );
    }

    @Override
    public Set<String> findByUsername( String username )
    {
        return groupsByUsername.get( username );
    }

    @Override
    public void create( GroupRecord group ) throws IllegalCredentialsException
    {
        synchronized (this)
        {
            // Check for existing group or token
            for ( GroupRecord other : groups.values() )
            {
                if ( other.name().equals( group.name() ) )
                {
                    throw new IllegalCredentialsException( "The specified group already exists" );
                }
            }

            groups.put( group.name(), group );

            populateUserMap( group );
        }
    }

    @Override
    public void update( GroupRecord existingGroup, GroupRecord updatedGroup ) throws ConcurrentModificationException
    {
        // Assert input is ok
        if ( !existingGroup.name().equals( updatedGroup.name() ) )
        {
            throw new IllegalArgumentException( "updatedGroup has a different name" );
        }

        synchronized (this)
        {
            boolean foundGroup = false;
            for ( GroupRecord other : groups.values() )
            {
                if ( other.equals( existingGroup ) )
                {
                    foundGroup = true;
                }
            }

            if ( !foundGroup )
            {
                throw new ConcurrentModificationException();
            }

            groups.put( updatedGroup.name(), updatedGroup );

            removeFromUserMap( existingGroup );
            populateUserMap( updatedGroup );
        }
    }

    @Override
    public boolean delete( GroupRecord group )
    {
        synchronized (this)
        {
            boolean removedGroup = groups.remove( group.name() ) != null;

            if ( removedGroup )
            {
                removeFromUserMap( group );
            }

            return removedGroup;
        }
    }

    @Override
    public int numberOfGroups()
    {
        return groups.size();
    }

    @Override
    public boolean isValidName( String name )
    {
        // This repo can store any name
        return true;
    }

    private void populateUserMap( GroupRecord group )
    {
        for ( String username : group.users() )
        {
            SortedSet<String> memberOfGroups = groupsByUsername.get( username );
            if ( memberOfGroups == null )
            {
                memberOfGroups = new ConcurrentSkipListSet<>();
                groupsByUsername.put( username, memberOfGroups );
            }
            memberOfGroups.add( group.name() );
        }
    }

    private void removeFromUserMap( GroupRecord group )
    {
        for ( String username : group.users() )
        {
            SortedSet<String> memberOfGroups = groupsByUsername.get( username );
            if ( memberOfGroups != null )
            {
                memberOfGroups.remove( group.name() );
            }
        }
    }

}

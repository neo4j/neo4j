/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.core;

import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.api.map.primitive.MutableObjectIntMap;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;
import org.eclipse.collections.impl.map.mutable.primitive.ObjectIntHashMap;
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.locks.StampedLock;

import org.neo4j.internal.kernel.api.NamedToken;

import static java.util.Collections.unmodifiableCollection;
import static org.neo4j.internal.kernel.api.TokenRead.NO_TOKEN;

/**
 * Token registry provide id -> TOKEN and name -> id mappings.
 * Name -> id mapping will be updated last since it's used to check if the token already exists.
 *
 * Implementation guarantees the atomicity of each method using internal locking.
 */
public class TokenRegistry
{
    private final String tokenType;
    private final StampedLock lock;
    private final MutableObjectIntMap<String> publicNameToId;
    private final MutableObjectIntMap<String> internalNameToId;
    private final MutableIntObjectMap<NamedToken> idToToken;

    public TokenRegistry( String tokenType )
    {
        this.tokenType = tokenType;
        lock = new StampedLock();
        publicNameToId = new ObjectIntHashMap<>();
        internalNameToId = new ObjectIntHashMap<>();
        idToToken = new IntObjectHashMap<>();
    }

    public String getTokenType()
    {
        return tokenType;
    }

    public void setInitialTokens( List<NamedToken> tokens )
    {
        long stamp = lock.writeLock();
        try
        {
            publicNameToId.clear();
            internalNameToId.clear();
            idToToken.clear();

            insertAllChecked( tokens );
        }
        finally
        {
            lock.unlockWrite( stamp );
        }
    }

    public void put( NamedToken token ) throws NonUniqueTokenException
    {
        long stamp = lock.writeLock();
        try
        {
            if ( idToToken.containsKey( token.id() ) )
            {
                throw new NonUniqueTokenException( tokenType, token.name(), token.id(), token.id() );
            }
            if ( token.isInternal() )
            {
                checkNameUniqueness( internalNameToId, token );
                internalNameToId.put( token.name(), token.id() );
            }
            else
            {
                checkNameUniqueness( publicNameToId, token );
                publicNameToId.put( token.name(), token.id() );
            }
            idToToken.put( token.id(), token );
        }
        finally
        {
            lock.unlockWrite( stamp );
        }
    }

    public Integer getId( String name )
    {
        return lockAndGetIdForName( publicNameToId, name );
    }

    public Integer getIdInternal( String name )
    {
        return lockAndGetIdForName( internalNameToId, name );
    }

    public NamedToken getToken( int id )
    {
        long stamp = lock.readLock();
        try
        {
            NamedToken token = idToToken.get( id );
            return token == null || token.isInternal() ? null : token;
        }
        finally
        {
            lock.unlockRead( stamp );
        }
    }

    public NamedToken getTokenInternal( int id )
    {
        long stamp = lock.readLock();
        try
        {
            NamedToken token = idToToken.get( id );
            return token != null && token.isInternal() ? token : null;
        }
        finally
        {
            lock.unlockRead( stamp );
        }
    }

    public Collection<NamedToken> allTokens()
    {
        long stamp = lock.readLock();
        try
        {
            // Likely nearly all tokens are returned here.
            List<NamedToken> list = new ArrayList<>( idToToken.size() );
            for ( NamedToken token : idToToken )
            {
                if ( !token.isInternal() )
                {
                    list.add( token );
                }
            }
            return unmodifiableCollection( list );
        }
        finally
        {
            lock.unlockRead( stamp );
        }
    }

    public Collection<NamedToken> allInternalTokens()
    {
        long stamp = lock.readLock();
        try
        {
            // Likely only a small fraction of all tokens are returned here.
            List<NamedToken> list = new ArrayList<>();
            for ( NamedToken token : idToToken )
            {
                if ( token.isInternal() )
                {
                    list.add( token );
                }
            }
            return unmodifiableCollection( list );
        }
        finally
        {
            lock.unlockRead( stamp );
        }
    }

    public int size()
    {
        long stamp = lock.readLock();
        try
        {
            return publicNameToId.size();
        }
        finally
        {
            lock.unlockRead( stamp );
        }
    }

    public int sizeInternal()
    {
        long stamp = lock.readLock();
        try
        {
            return internalNameToId.size();
        }
        finally
        {
            lock.unlockRead( stamp );
        }
    }

    public void putAll( List<NamedToken> tokens ) throws NonUniqueTokenException
    {
        long stamp = lock.writeLock();
        try
        {
            insertAllChecked( tokens );
        }
        finally
        {
            lock.unlockWrite( stamp );
        }
    }

    private void insertAllChecked( List<NamedToken> tokens )
    {
        MutableObjectIntMap<String> uniquePublicNames = new ObjectIntHashMap<>();
        MutableObjectIntMap<String> uniqueInternalNames = new ObjectIntHashMap<>();
        MutableIntSet uniqueIds = new IntHashSet();

        for ( NamedToken token : tokens )
        {
            if ( token.isInternal() )
            {
                checkNameUniqueness( uniqueInternalNames, token );
                checkNameUniqueness( internalNameToId, token );
                uniqueInternalNames.put( token.name(), token.id() );
            }
            else
            {
                checkNameUniqueness( uniquePublicNames, token );
                checkNameUniqueness( publicNameToId, token );
                uniquePublicNames.put( token.name(), token.id() );
            }
            if ( !uniqueIds.add( token.id() ) || idToToken.containsKey( token.id() ) )
            {
                throw new NonUniqueTokenException( tokenType, token.name(), token.id(), token.id() );
            }
        }

        for ( NamedToken token : tokens )
        {
            insertUnchecked( token );
        }
    }

    private void checkNameUniqueness( MutableObjectIntMap<String> namesToId, NamedToken token )
    {
        if ( namesToId.containsKey( token.name() ) )
        {
            int existingKey = namesToId.get( token.name() );
            throw new NonUniqueTokenException( tokenType, token.name(), token.id(), existingKey );
        }
    }

    private void insertUnchecked( NamedToken token )
    {
        idToToken.put( token.id(), token );
        if ( token.isInternal() )
        {
            internalNameToId.put( token.name(), token.id() );
        }
        else
        {
            publicNameToId.put( token.name(), token.id() );
        }
    }

    private Integer lockAndGetIdForName( MutableObjectIntMap<String> nameToId, String name )
    {
        long stamp = lock.readLock();
        try
        {
            int id = nameToId.getIfAbsent( name, NO_TOKEN );
            return id == NO_TOKEN ? null : id;
        }
        finally
        {
            lock.unlockRead( stamp );
        }
    }
}

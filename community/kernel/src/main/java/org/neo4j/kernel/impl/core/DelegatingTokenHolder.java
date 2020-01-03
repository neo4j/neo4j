/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.eclipse.collections.api.set.primitive.IntSet;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.impl.map.mutable.primitive.IntIntHashMap;
import org.eclipse.collections.impl.map.mutable.primitive.ObjectIntHashMap;
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet;

import java.util.ArrayList;
import java.util.List;
import java.util.function.IntPredicate;

import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.internal.kernel.api.NamedToken;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.ReadOnlyDbException;

import static org.neo4j.function.Predicates.ALWAYS_TRUE_INT;

/**
 * Keeps a registry of tokens using {@link TokenRegistry}.
 * When asked for a token that isn't in the registry, delegates to a {@link TokenCreator} to create the token,
 * then stores it in the registry.
 */
public class DelegatingTokenHolder extends AbstractTokenHolderBase
{
    private final TokenCreator tokenCreator;

    public DelegatingTokenHolder( TokenCreator tokenCreator, String tokenType )
    {
        super( new TokenRegistry( tokenType ) );
        this.tokenCreator = tokenCreator;
    }

    /**
     * Create and put new token in cache.
     *
     * @param name token name
     * @return newly created token id
     * @throws KernelException
     */
    @Override
    protected synchronized int createToken( String name ) throws KernelException
    {
        Integer id = tokenRegistry.getId( name );
        if ( id != null )
        {
            return id;
        }

        id = tokenCreator.createToken( name );
        try
        {
            tokenRegistry.put( new NamedToken( name, id ) );
        }
        catch ( NonUniqueTokenException e )
        {
            throw new IllegalStateException( "Newly created token should be unique.", e );
        }
        return id;
    }

    @Override
    public void getOrCreateIds( String[] names, int[] ids )
    {
        if ( names.length != ids.length )
        {
            throw new IllegalArgumentException( "Name and id arrays must have the same length." );
        }
        // Assume all tokens exist and try to resolve them. Break out on the first missing token.
        boolean hasUnresolvedTokens = resolveIds( names, ids, ALWAYS_TRUE_INT );

        if ( hasUnresolvedTokens )
        {
            createMissingTokens( names, ids );
        }
    }

    private synchronized void createMissingTokens( String[] names, int[] ids )
    {
        // We redo the resolving under the lock, to make sure that these ids are really missing, and won't be
        // created concurrently with us.
        MutableIntSet unresolvedIndexes = new IntHashSet();
        resolveIds( names, ids, i -> !unresolvedIndexes.add( i ) );
        if ( !unresolvedIndexes.isEmpty() )
        {
            // We still have unresolved ids to create.
            ObjectIntHashMap<String> createdTokens = createUnresolvedTokens( unresolvedIndexes, names, ids );
            List<NamedToken> createdTokensList = new ArrayList<>( createdTokens.size() );
            createdTokens.forEachKeyValue( ( name, index ) ->
                    createdTokensList.add( new NamedToken( name, ids[index] ) ) );

            tokenRegistry.putAll( createdTokensList );
        }
    }

    private ObjectIntHashMap<String> createUnresolvedTokens( IntSet unresolvedIndexes, String[] names, int[] ids )
    {
        try
        {
            // First, we need to filter out all of the tokens that are already resolved, so we only create tokens for
            // indexes that are in the unresolvedIndexes set.
            // However, we also need to deal with duplicate token names. For any token index we decide needs to have a
            // token created, we will add a mapping from the token name, to the ids-index into which the token id will
            // be written. This is the 'createdTokens' map. It maps token names to indexes into the 'ids' array.
            // If we find that the 'created'Tokens' map already has an entry for a given name, then that name is a
            // duplicate, and we will need to "remap" it later, by reading the token id from the correct index in the
            // 'ids' array, and storing it at the indexes of the duplicates. This is what the 'remappingIndexes' map is
            // for. This is a map from 'a' to 'b', where both 'a' and 'b' are indexes into the 'ids' array, and where
            // the corresponding name for 'a' is a duplicate of the name for 'b', and where we have already decided
            // that we will create a token id for index 'b'. After the token ids have been created, we go through the
            // 'remappingIndexes' map, and for every '(a,b)' entry, we store the token id created for 'b' and 'ids'
            // index 'a'.
            ObjectIntHashMap<String> createdTokens = new ObjectIntHashMap<>();
            IntIntHashMap remappingIndexes = new IntIntHashMap();
            IntPredicate tokenCreateFilter = index ->
            {
                boolean needsCreate = unresolvedIndexes.contains( index );
                if ( needsCreate )
                {
                    // The name at this index is unresolved.
                    String name = names[index];
                    int creatingIndex = createdTokens.getIfAbsentPut( name, index );
                    if ( creatingIndex != index )
                    {
                        // This entry has a duplicate name, so we need to remap this entry instead of creating a token
                        // for it.
                        remappingIndexes.put( index, creatingIndex );
                        needsCreate = false;
                    }
                }
                return needsCreate;
            };

            // Create tokens for all the indexes that we don't filter out.
            tokenCreator.createTokens( names, ids, tokenCreateFilter );

            // Remap duplicate tokens to the token id we created for the first instance of any duplicate token name.
            if ( remappingIndexes.notEmpty() )
            {
                remappingIndexes.forEachKeyValue( ( index, creatingIndex ) -> ids[index] = ids[creatingIndex] );
            }

            return createdTokens;
        }
        catch ( ReadOnlyDbException e )
        {
            throw new TransactionFailureException( e.getMessage(), e );
        }
        catch ( Throwable e )
        {
            throw new TransactionFailureException( "Could not create tokens.", e );
        }
    }
}

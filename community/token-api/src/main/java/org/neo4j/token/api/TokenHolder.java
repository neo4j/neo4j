/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.token.api;

import java.util.List;
import org.neo4j.exceptions.KernelException;

public interface TokenHolder {
    String TYPE_PROPERTY_KEY = "PropertyKey";
    String TYPE_RELATIONSHIP_TYPE = "RelationshipType";
    String TYPE_LABEL = "Label";

    /**
     * Clear the internal token registry and repopulate it with the given collection of tokens.
     * <p>
     * The tokens may be a mix of public and internal tokens.
     *
     * @param tokens The set of initial tokens to populate this token holder with.
     * @throws NonUniqueTokenException If there are duplicate names or ids amongst the given tokens.
     */
    void setInitialTokens(List<NamedToken> tokens);

    /**
     * Add the given token to the internal token registry. The token maybe be either a public or an internal one.
     *
     * @param token The token to add.
     * @param atomic whether there should be extra care taken to make this addition look atomic to readers.
     * @throws NonUniqueTokenException if the token conflicts with an existing token on id, or on name.
     */
    void addToken(NamedToken token, boolean atomic);

    /**
     * @see #addToken(NamedToken, boolean) with {@code atomic} = {@code true}.
     */
    default void addToken(NamedToken token) {
        addToken(token, true);
    }

    /**
     * Get the id of the public token by the given name, or create a new id for the token if it does not have one already,
     * and then return that id.
     * <p>
     * This method is thread-safe, and will ensure that distinct tokens will not have multiple ids allocated for them.
     *
     * @param name The name of the token to get the id for.
     * @return The (possibly newly created) id of the given token.
     */
    int getOrCreateId(String name) throws KernelException;

    /**
     * Resolve the ids of the given token {@code names} into the array for {@code ids}.
     * <p>
     * Any tokens that don't already have an id will have one created for it.
     * <p>
     * Note that this only looks at public tokens. For internal tokens, use {@link #getOrCreateInternalIds(String[], int[])}.
     */
    void getOrCreateIds(String[] names, int[] ids) throws KernelException;

    /**
     * Get the (public) token that has the given id, or throw {@link TokenNotFoundException}.
     * <p>
     * The exception will be thrown even if an internal token exists with that id.
     * <p>
     * To get an internal token, see {@link #getInternalTokenById(int)}.
     */
    NamedToken getTokenById(int id) throws TokenNotFoundException;

    /**
     * Returns the id, or {@link TokenConstants#NO_TOKEN} if no token with this name exists.
     */
    int getIdByName(String name);

    /**
     * Resolve the ids of the given token {@code names} into the array for {@code ids}.
     * <p>
     * Any tokens that don't already have an id will not be resolved, and the corresponding entry in the {@code ids}
     * array will be left untouched. If you wish for those unresolved id entries to end up with the {@link TokenConstants#NO_TOKEN}
     * value, you must first fill the array with that value before calling this method.
     * <p>
     * This method will not resolve internal tokens; only public ones.
     *
     * @return {@code true} if some of the token names could not be resolved, {@code false} otherwise.
     */
    boolean getIdsByNames(String[] names, int[] ids);

    /**
     * Get an iterator of all public tokens. No internal tokens are returned by this iterator.
     */
    Iterable<NamedToken> getAllTokens();

    /**
     * Get the type of the tokens held in this holder.
     * @return the type of tokens in this holder.
     */
    String getTokenType();

    /**
     * Tests whether or not the given token id is in use.
     * @param id the id to test for.
     * @return {@code true} if the given token id is in use.
     */
    boolean hasToken(int id);

    /**
     * @return the number of public tokens currently in this token holder.
     */
    int size();

    /**
     * This is the same as {@link #getOrCreateIds(String[], int[])}, but for internal tokens.
     * <p>
     * This method does not take public tokens into consideration.
     */
    void getOrCreateInternalIds(String[] names, int[] ids) throws KernelException;

    /**
     * This is the same as {@link #getTokenById(int)}, but for internal tokens.
     * <p>
     * The {@link TokenNotFoundException} exception will be thrown even if a public token exists with that id.
     */
    NamedToken getInternalTokenById(int id) throws TokenNotFoundException;
}

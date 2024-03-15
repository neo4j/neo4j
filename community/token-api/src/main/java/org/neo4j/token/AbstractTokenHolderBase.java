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
package org.neo4j.token;

import static org.neo4j.function.Predicates.ALWAYS_FALSE_INT;

import java.util.List;
import java.util.function.IntPredicate;
import org.neo4j.exceptions.KernelException;
import org.neo4j.token.api.NamedToken;
import org.neo4j.token.api.TokenConstants;
import org.neo4j.token.api.TokenHolder;
import org.neo4j.token.api.TokenNotFoundException;

public abstract class AbstractTokenHolderBase implements TokenHolder {
    final TokenRegistry tokenRegistry;

    public AbstractTokenHolderBase(TokenRegistry tokenRegistry) {
        this.tokenRegistry = tokenRegistry;
    }

    @Override
    public void setInitialTokens(List<NamedToken> tokens) {
        tokenRegistry.setInitialTokens(tokens);
    }

    @Override
    public void addToken(NamedToken token, boolean atomic) {
        tokenRegistry.put(token);
    }

    @Override
    public int getOrCreateId(String name) throws KernelException {
        return innerGetOrCreateId(name, false);
    }

    protected int innerGetOrCreateId(String name, boolean internal) throws KernelException {
        Integer id = innerGetId(name, internal);
        if (id != null) {
            return id;
        }

        // Let's create it
        return createToken(name, internal);
    }

    @Override
    public NamedToken getTokenById(int id) throws TokenNotFoundException {
        NamedToken result = tokenRegistry.getToken(id);
        if (result == null) {
            throw new TokenNotFoundException("Token for id " + id);
        }
        return result;
    }

    @Override
    public int getIdByName(String name) {
        Integer id = tokenRegistry.getId(name);
        if (id == null) {
            return TokenConstants.NO_TOKEN;
        }
        return id;
    }

    @Override
    public boolean getIdsByNames(String[] names, int[] ids) {
        return resolveIds(names, ids, false, ALWAYS_FALSE_INT);
    }

    @Override
    public Iterable<NamedToken> getAllTokens() {
        return tokenRegistry.allTokens();
    }

    @Override
    public String getTokenType() {
        return tokenRegistry.getTokenType();
    }

    @Override
    public boolean hasToken(int id) {
        return tokenRegistry.hasToken(id);
    }

    @Override
    public int size() {
        return tokenRegistry.size();
    }

    @Override
    public NamedToken getInternalTokenById(int id) throws TokenNotFoundException {
        NamedToken result = tokenRegistry.getTokenInternal(id);
        if (result == null) {
            NamedToken alternative = tokenRegistry.getToken(id);
            throw new TokenNotFoundException("Internal token for id " + id + " not found"
                    + (alternative != null ? ", but a public token exists on that id: " + alternative + "." : "."));
        }
        return result;
    }

    protected abstract int createToken(String tokenName, boolean internal) throws KernelException;

    boolean resolveIds(String[] names, int[] ids, boolean internal, IntPredicate unresolvedIndexCheck) {
        boolean foundUnresolvable = false;
        for (int i = 0; i < ids.length; i++) {
            Integer id = innerGetId(names[i], internal);
            if (id != null) {
                ids[i] = id;
            } else {
                foundUnresolvable = true;
                if (unresolvedIndexCheck.test(i)) {
                    // If the check returns `true`, it's a signal that we should stop early.
                    break;
                }
            }
        }
        return foundUnresolvable;
    }

    private Integer innerGetId(String name, boolean internal) {
        return internal ? tokenRegistry.getIdInternal(name) : tokenRegistry.getId(name);
    }
}

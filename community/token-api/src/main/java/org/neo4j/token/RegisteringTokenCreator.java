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

import java.util.ArrayList;
import java.util.List;
import java.util.function.IntPredicate;
import org.neo4j.exceptions.KernelException;
import org.neo4j.token.api.NamedToken;

public class RegisteringTokenCreator implements TokenCreator {
    private final TokenRegistry registry;
    private final TokenCreator creator;

    public RegisteringTokenCreator(TokenRegistry registry, TokenCreator creator) {
        this.registry = registry;
        this.creator = creator;
    }

    @Override
    public int createToken(String name, boolean internal) throws KernelException {
        int id = creator.createToken(name, internal);
        registry.put(new NamedToken(name, id, internal));
        return id;
    }

    @Override
    public void createTokens(String[] names, int[] ids, boolean internal, IntPredicate indexFilter)
            throws KernelException {
        creator.createTokens(names, ids, internal, indexFilter);
        List<NamedToken> createdTokensList = new ArrayList<>();
        for (int i = 0; i < names.length; i++) {
            if (indexFilter.test(i)) {
                createdTokensList.add(new NamedToken(names[i], ids[i], internal));
            }
        }
        registry.putAll(createdTokensList);
    }
}

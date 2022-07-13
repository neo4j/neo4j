/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.kernel.impl.api;

import java.util.function.Predicate;
import org.neo4j.internal.helpers.collection.LfuCache;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.string.Globbing;
import org.neo4j.token.TokenHolders;

class TokenHoldersIdLookup implements LoginContext.IdLookup {
    private static final int LOOKUP_CACHE_SIZE = 100;
    private final TokenHolders tokens;
    private final GlobalProcedures globalProcedures;
    private final LfuCache<String, int[]> proceduresLookupCache = new LfuCache<>("procedures", LOOKUP_CACHE_SIZE);
    private final LfuCache<String, int[]> functionsLookupCache = new LfuCache<>("functions", LOOKUP_CACHE_SIZE);
    private final LfuCache<String, int[]> aggregationFunctionsLookupCache =
            new LfuCache<>("aggregationFunctions", LOOKUP_CACHE_SIZE);

    TokenHoldersIdLookup(TokenHolders tokens, GlobalProcedures globalProcedures) {
        this.tokens = tokens;
        this.globalProcedures = globalProcedures;
    }

    @Override
    public int getPropertyKeyId(String name) {
        return tokens.propertyKeyTokens().getIdByName(name);
    }

    @Override
    public int getLabelId(String name) {
        return tokens.labelTokens().getIdByName(name);
    }

    @Override
    public int getRelTypeId(String name) {
        return tokens.relationshipTypeTokens().getIdByName(name);
    }

    @Override
    public int[] getProcedureIds(String procedureGlobbing) {
        int[] cachedResult = proceduresLookupCache.get(procedureGlobbing);
        if (cachedResult != null) {
            return cachedResult;
        }
        Predicate<String> matcherPredicate = Globbing.globPredicate(procedureGlobbing);
        int[] data = globalProcedures.getIdsOfProceduresMatching(
                p -> matcherPredicate.test(p.signature().name().toString()));
        proceduresLookupCache.put(procedureGlobbing, data);
        return data;
    }

    @Override
    public int[] getAdminProcedureIds() {
        return globalProcedures.getIdsOfProceduresMatching(p -> p.signature().admin());
    }

    @Override
    public int[] getFunctionIds(String functionGlobbing) {
        int[] cachedResult = functionsLookupCache.get(functionGlobbing);
        if (cachedResult != null) {
            return cachedResult;
        }
        Predicate<String> matcherPredicate = Globbing.globPredicate(functionGlobbing);
        int[] data = globalProcedures.getIdsOfFunctionsMatching(
                f -> matcherPredicate.test(f.signature().name().toString()));
        functionsLookupCache.put(functionGlobbing, data);
        return data;
    }

    @Override
    public int[] getAggregatingFunctionIds(String functionGlobbing) {
        int[] cachedResult = aggregationFunctionsLookupCache.get(functionGlobbing);
        if (cachedResult != null) {
            return cachedResult;
        }
        Predicate<String> matcherPredicate = Globbing.globPredicate(functionGlobbing);
        int[] data = globalProcedures.getIdsOfAggregatingFunctionsMatching(
                f -> matcherPredicate.test(f.signature().name().toString()));
        aggregationFunctionsLookupCache.put(functionGlobbing, data);
        return data;
    }
}

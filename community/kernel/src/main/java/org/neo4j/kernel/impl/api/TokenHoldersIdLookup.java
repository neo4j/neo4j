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
package org.neo4j.kernel.impl.api;

import java.util.function.BooleanSupplier;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.procedure.ProcedureView;
import org.neo4j.token.TokenHolders;

class TokenHoldersIdLookup implements LoginContext.IdLookup {
    private final TokenHolders tokens;
    private final ProcedureView view;

    private final BooleanSupplier isStale;

    TokenHoldersIdLookup(TokenHolders tokens, ProcedureView view, BooleanSupplier isStale) {
        this.tokens = tokens;
        this.view = view;
        this.isStale = isStale;
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
        return view.getProcedureIds(procedureGlobbing);
    }

    @Override
    public int[] getAdminProcedureIds() {
        return view.getAdminProcedureIds();
    }

    @Override
    public int[] getFunctionIds(String functionGlobbing) {
        return view.getFunctionIds(functionGlobbing);
    }

    @Override
    public int[] getAggregatingFunctionIds(String functionGlobbing) {
        return view.getAggregatingFunctionIds(functionGlobbing);
    }

    @Override
    public boolean isCachableLookup() {
        return true;
    }

    @Override
    public boolean isStale() {
        return isStale.getAsBoolean();
    }
}

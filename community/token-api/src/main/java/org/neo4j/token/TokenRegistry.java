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

import static java.util.Collections.unmodifiableCollection;
import static org.neo4j.token.api.TokenConstants.NO_TOKEN;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.api.map.primitive.MutableObjectIntMap;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.impl.factory.primitive.IntObjectMaps;
import org.eclipse.collections.impl.factory.primitive.ObjectIntMaps;
import org.eclipse.collections.impl.map.mutable.primitive.ObjectIntHashMap;
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet;
import org.neo4j.token.api.NamedToken;
import org.neo4j.token.api.NonUniqueTokenException;

/**
 * Token registry provide id -> TOKEN and name -> id mappings.
 * Name -> id mapping will be updated last since it's used to check if the token already exists.
 *
 * Implementation guarantees the atomicity of each method using internal locking.
 */
public class TokenRegistry {
    private final String tokenType;
    private volatile Registries registries;

    public TokenRegistry(String tokenType) {
        this.tokenType = tokenType;
        this.registries = new Registries();
    }

    public String getTokenType() {
        return tokenType;
    }

    public synchronized void setInitialTokens(List<NamedToken> tokens) {
        registries = insertAllChecked(tokens, new Registries());
    }

    public void put(NamedToken token) {
        put(token, true);
    }

    public synchronized void put(NamedToken token, boolean atomic) {
        Registries reg = this.registries;
        if (reg.idToToken.containsKey(token.id())) {
            NamedToken existingToken = reg.idToToken.get(token.id());
            if (token.equals(existingToken)) {
                return; // Adding the same token twice is okay
            }
            throw new NonUniqueTokenException(tokenType, token, existingToken);
        }

        if (atomic) {
            reg = reg.copy();
        }
        if (token.isInternal()) {
            checkNameUniqueness(reg.internalNameToId, token, reg);
            reg.internalNameToId.put(token.name(), token.id());
        } else {
            checkNameUniqueness(reg.publicNameToId, token, reg);
            reg.publicNameToId.put(token.name(), token.id());
        }
        reg.idToToken.put(token.id(), token);
        this.registries = reg;
    }

    public synchronized void putAll(List<NamedToken> tokens) {
        registries = insertAllChecked(tokens, registries.copy());
    }

    public int getId(String name) {
        return getIdForName(registries.publicNameToId, name);
    }

    public int getIdInternal(String name) {
        return getIdForName(registries.internalNameToId, name);
    }

    public NamedToken getToken(int id) {
        NamedToken token = registries.idToToken.get(id);
        return token == null || token.isInternal() ? null : token;
    }

    public NamedToken getTokenInternal(int id) {
        NamedToken token = registries.idToToken.get(id);
        return token != null && token.isInternal() ? token : null;
    }

    public Collection<NamedToken> allTokens() {
        // Likely nearly all tokens are returned here.
        Registries reg = this.registries;
        List<NamedToken> list = new ArrayList<>(reg.idToToken.size());
        for (NamedToken token : reg.idToToken) {
            if (!token.isInternal()) {
                list.add(token);
            }
        }
        return unmodifiableCollection(list);
    }

    public Collection<NamedToken> allInternalTokens() {
        // Likely only a small fraction of all tokens are returned here.
        Registries reg = this.registries;
        List<NamedToken> list = new ArrayList<>();
        for (NamedToken token : reg.idToToken) {
            if (token.isInternal()) {
                list.add(token);
            }
        }
        return unmodifiableCollection(list);
    }

    public boolean hasToken(int id) {
        return registries.idToToken.containsKey(id);
    }

    public int size() {
        return registries.publicNameToId.size();
    }

    public int sizeInternal() {
        return registries.internalNameToId.size();
    }

    private Registries insertAllChecked(List<NamedToken> tokens, Registries registries) {
        MutableObjectIntMap<String> uniquePublicNames = new ObjectIntHashMap<>();
        MutableObjectIntMap<String> uniqueInternalNames = new ObjectIntHashMap<>();
        MutableIntSet uniqueIds = new IntHashSet();

        for (NamedToken token : tokens) {
            if (token.isInternal()) {
                checkNameUniqueness(uniqueInternalNames, token, registries);
                checkNameUniqueness(registries.internalNameToId, token, registries);
                uniqueInternalNames.put(token.name(), token.id());
            } else {
                checkNameUniqueness(uniquePublicNames, token, registries);
                checkNameUniqueness(registries.publicNameToId, token, registries);
                uniquePublicNames.put(token.name(), token.id());
            }
            if (!uniqueIds.add(token.id()) || registries.idToToken.containsKey(token.id())) {
                NamedToken existingToken = registries.idToToken.get(token.id());
                throw new NonUniqueTokenException(tokenType, token, existingToken);
            }
            insertUnchecked(token, registries);
        }

        return registries;
    }

    private void checkNameUniqueness(MutableObjectIntMap<String> namesToId, NamedToken token, Registries registries) {
        if (namesToId.containsKey(token.name())) {
            int existingKey = namesToId.get(token.name());
            NamedToken existingToken = registries.idToToken.get(existingKey);
            throw new NonUniqueTokenException(tokenType, token, existingToken);
        }
    }

    private static void insertUnchecked(NamedToken token, Registries registries) {
        registries.idToToken.put(token.id(), token);
        if (token.isInternal()) {
            registries.internalNameToId.put(token.name(), token.id());
        } else {
            registries.publicNameToId.put(token.name(), token.id());
        }
    }

    private static int getIdForName(MutableObjectIntMap<String> nameToId, String name) {
        return nameToId.getIfAbsent(name, NO_TOKEN);
    }

    private static final class Registries {
        private final MutableObjectIntMap<String> publicNameToId;
        private final MutableObjectIntMap<String> internalNameToId;
        private final MutableIntObjectMap<NamedToken> idToToken;

        private Registries() {
            this(ObjectIntMaps.mutable.empty(), ObjectIntMaps.mutable.empty(), IntObjectMaps.mutable.empty());
        }

        private Registries(
                MutableObjectIntMap<String> publicNameToId,
                MutableObjectIntMap<String> internalNameToId,
                MutableIntObjectMap<NamedToken> idToToken) {
            this.publicNameToId = publicNameToId;
            this.internalNameToId = internalNameToId;
            this.idToToken = idToToken;
        }

        private Registries copy() {
            return new Registries(
                    ObjectIntMaps.mutable.withAll(publicNameToId),
                    ObjectIntMaps.mutable.withAll(internalNameToId),
                    IntObjectMaps.mutable.withAll(idToToken));
        }
    }
}

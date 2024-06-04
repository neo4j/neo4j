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
package org.neo4j.procedure.impl;

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.kernel.api.CypherScope;

/**
 * Simple in memory store for procedures.
 *
 * The implementation preserves ids for QualifiedName's in order
 * to allow for entries to be overwritten.
 *
 * Should only be accessed from a single thread
 * @param <T> the type to be stored
 */
class ProcedureHolder<T> {
    private final Map<QualifiedName, int[]> nameToEntries;
    private final Map<QualifiedName, int[]> caseInsensitiveName2Entries;

    private static int UNUSED_REFERENCE = -1;
    private final List<Object> store;

    private static final Object TOMBSTONE = new Object() {
        @Override
        public String toString() {
            return "TOMBSTONE";
        }
    };

    public ProcedureHolder() {
        this(new HashMap<>(), new HashMap<>(), new ArrayList<>());
    }

    private ProcedureHolder(
            Map<QualifiedName, int[]> nameToId, Map<QualifiedName, int[]> caseInsensitiveName2Id, List<Object> store) {
        this.nameToEntries = nameToId;
        this.caseInsensitiveName2Entries = caseInsensitiveName2Id;
        this.store = store;
    }

    T getByKey(QualifiedName name, CypherScope scope) {
        int[] ids = name2entry(name);
        if (ids == null) {
            return null;
        }
        int reference = ids[scope.ordinal()];
        if (reference == UNUSED_REFERENCE) {
            return null;
        }
        Object value = store.get(reference);
        if (value == TOMBSTONE) {
            return null;
        }

        return (T) value;
    }

    T getById(int id) {
        Object element = store.get(id);
        if (element == TOMBSTONE) {
            return null;
        }

        return (T) element;
    }

    int put(QualifiedName name, Set<CypherScope> scopes, T item, boolean caseInsensitive) {
        int[] entry = name2entry(name);
        int reference = UNUSED_REFERENCE;

        if (entry != null) {
            // If the item already exists, then there is at least one scope set.
            if (hasDifferentScopes(entry, scopes)) {
                // If there is a different set of scopes, then we will need to add a new item.
                reference = store.size();
                store.add(item);
                for (var scope : scopes) {
                    entry[scope.ordinal()] = reference;
                }
            } else {
                // The scopes are the same, then we go ahead and update the items
                for (var scope : scopes) {
                    reference = entry[scope.ordinal()];
                    store.set(reference, item);
                }
            }

        } else {
            reference = store.size();
            entry = makeEntry(scopes, reference);
            nameToEntries.put(name, entry);
            store.add(item);
        }

        // Update case sensitivity
        var lowercaseName = toLowerCaseName(name);
        if (caseInsensitive) {
            caseInsensitiveName2Entries.put(lowercaseName, entry);
        } else {
            caseInsensitiveName2Entries.remove(lowercaseName);
        }

        assert reference != UNUSED_REFERENCE;
        return reference;
    }

    /**
     * Create a tombstone:d copy of the ProcedureHolder.
     *
     * @param src The source ProcedureHolder from which the copy is made.
     * @param which The ids that should be tombstone:d, if any.
     *
     * @return A new ProcedureHolder
     */
    public static <T> ProcedureHolder<T> tombstone(ProcedureHolder<T> src, Predicate<QualifiedName> which) {
        requireNonNull(which);

        var ret = new ProcedureHolder<T>();
        IntHashSet matches = new IntHashSet();
        for (var entry : src.nameToEntries.entrySet()) {
            if (which.test(entry.getKey())) {
                matches.addAll(entry.getValue());
            }
        }

        for (int i = 0; i < src.store.size(); i++) {
            if (matches.contains(i)) {
                ret.store.add(TOMBSTONE);
            } else {
                ret.store.add(src.store.get(i));
            }
        }

        ret.caseInsensitiveName2Entries.putAll(src.caseInsensitiveName2Entries);
        ret.nameToEntries.putAll(src.nameToEntries);

        return ret;
    }

    int idOfKey(QualifiedName name, CypherScope scope) {
        int[] entry = name2entry(name);

        if (entry == null) {
            throw new NoSuchElementException();
        }

        int reference = entry[scope.ordinal()];
        if (reference == UNUSED_REFERENCE) {
            throw new NoSuchElementException();
        }

        if (store.get(reference) == TOMBSTONE) {
            throw new NoSuchElementException();
        }

        return reference;
    }

    void forEach(BiConsumer<Integer, T> consumer) {
        for (int i = 0; i < store.size(); i++) {
            var item = store.get(i);
            if (item != TOMBSTONE) {
                consumer.accept(i, (T) item);
            }
        }
    }

    boolean contains(QualifiedName name, CypherScope scope) {
        return getByKey(name, scope) != null;
    }

    private int[] name2entry(QualifiedName name) {
        int[] entry = nameToEntries.get(name);
        if (entry == null) { // Did not find it in the case sensitive lookup - let's check for case insensitive objects
            QualifiedName lowerCaseName = toLowerCaseName(name);
            entry = caseInsensitiveName2Entries.get(lowerCaseName);
        }

        return entry;
    }

    private QualifiedName toLowerCaseName(QualifiedName name) {
        String[] oldNs = name.namespace();
        String[] lowerCaseNamespace = new String[oldNs.length];
        for (int i = 0; i < oldNs.length; i++) {
            lowerCaseNamespace[i] = oldNs[i].toLowerCase(Locale.ROOT);
        }
        String lowercaseName = name.name().toLowerCase(Locale.ROOT);
        return new QualifiedName(lowerCaseNamespace, lowercaseName);
    }

    public void unregister(QualifiedName name) {
        int[] entry = name2entry(name);
        if (entry == null) {
            return;
        }
        for (int reference : entry) {
            if (reference != UNUSED_REFERENCE) {
                store.set(reference, TOMBSTONE);
            }
        }
    }
    /**
     * Create an immutable copy of the ProcedureHolder
     *
     * @param ref The source {@link ProcedureHolder} to copy.
     *
     * @return an immutable copy of the source
     **/
    public static <T> ProcedureHolder<T> copyOf(ProcedureHolder<T> ref) {
        return new ProcedureHolder<>(
                Map.copyOf(ref.nameToEntries), Map.copyOf(ref.caseInsensitiveName2Entries), List.copyOf(ref.store));
    }

    private static boolean hasDifferentScopes(int[] entry, Set<CypherScope> scopes) {
        for (var scope : CypherScope.ALL_SCOPES) {
            if (entry[scope.ordinal()] != UNUSED_REFERENCE && !scopes.contains(scope)) {
                return true;
            }
        }
        return false;
    }

    private static int[] makeEntry(Set<CypherScope> scopes, int reference) {
        int[] ids = new int[CypherScope.ALL_SCOPES.size()];
        for (var s : CypherScope.ALL_SCOPES) {
            if (scopes.contains(s)) {
                ids[s.ordinal()] = reference;
            } else {
                ids[s.ordinal()] = UNUSED_REFERENCE;
            }
        }
        return ids;
    }
}

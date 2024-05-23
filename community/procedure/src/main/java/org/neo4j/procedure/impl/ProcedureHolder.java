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
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.neo4j.function.Predicates;
import org.neo4j.internal.kernel.api.procs.QualifiedName;

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
    private final Map<QualifiedName, Integer> nameToId;
    private final Map<QualifiedName, Integer> caseInsensitiveName2Id;
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
            Map<QualifiedName, Integer> nameToId,
            Map<QualifiedName, Integer> caseInsensitiveName2Id,
            List<Object> store) {
        this.nameToId = nameToId;
        this.caseInsensitiveName2Id = caseInsensitiveName2Id;
        this.store = store;
    }

    T get(QualifiedName name) {
        Integer id = name2Id(name);
        if (id == null) {
            return null;
        }
        Object value = store.get(id);
        if (value == TOMBSTONE) {
            return null;
        }

        return (T) value;
    }

    T get(int id) {
        Object element = store.get(id);
        if (element == TOMBSTONE) {
            return null;
        }

        return (T) element;
    }

    int put(QualifiedName name, T item, boolean caseInsensitive) {
        Integer id = name2Id(name);

        // Existing entry -> preserve ids
        if (id != null) {
            store.set(id, item);
        } else {
            id = store.size();
            nameToId.put(name, id);
            store.add(item);
        }

        // Update case sensitivity
        var lowercaseName = toLowerCaseName(name);
        if (caseInsensitive) {
            caseInsensitiveName2Id.put(lowercaseName, id);
        } else {
            caseInsensitiveName2Id.remove(lowercaseName);
        }

        return id;
    }

    /**
     * Create a tombstone:d copy of the ProcedureHolder.
     *
     * @param src The source ProcedureHolder from which the copy is made.
     * @param which The ids that should be preserved, if any.
     *
     * @return A new ProcedureHolder
     */
    public static <T> ProcedureHolder<T> tombstone(ProcedureHolder<T> src, Predicate<QualifiedName> which) {
        requireNonNull(which);

        var ret = new ProcedureHolder<T>();

        Set<Integer> matches = src.nameToId.entrySet().stream()
                .filter(entry -> which.test(entry.getKey()))
                .map(Entry::getValue)
                .collect(Collectors.toSet());

        for (int i = 0; i < src.store.size(); i++) {
            if (matches.contains(i)) {
                ret.store.add(TOMBSTONE);
            } else {
                ret.store.add(src.store.get(i));
            }
        }

        ret.caseInsensitiveName2Id.putAll(src.caseInsensitiveName2Id);
        ret.nameToId.putAll(src.nameToId);

        return ret;
    }

    int idOf(QualifiedName name) {
        Integer id = name2Id(name);

        if (id == null || store.get(id) == TOMBSTONE) {
            throw new NoSuchElementException();
        }

        return id;
    }

    List<T> all() {
        // In the general case, the procedure list is upper bounded by the store size,
        // but since tombstone:d elements are rare, the size will in all likelihood be
        // equal to the store size.
        var lst = new ArrayList<T>(store.size());
        forEach((id, item) -> lst.add(item), Predicates.alwaysTrue());
        return lst;
    }

    void forEach(BiConsumer<Integer, T> consumer, Predicate<T> filter) {
        for (int i = 0; i < store.size(); i++) {
            var item = store.get(i);
            if (item != TOMBSTONE && filter.test((T) item)) {
                consumer.accept(i, (T) item);
            }
        }
    }

    boolean contains(QualifiedName name) {
        return get(name) != null;
    }

    private Integer name2Id(QualifiedName name) {
        Integer id = nameToId.get(name);
        if (id == null) { // Did not find it in the case sensitive lookup - let's check for case insensitive objects
            QualifiedName lowerCaseName = toLowerCaseName(name);
            id = caseInsensitiveName2Id.get(lowerCaseName);
        }

        return id;
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
        Integer id = name2Id(name);
        if (id != null) {
            store.set(id, TOMBSTONE);
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
                Map.copyOf(ref.nameToId), Map.copyOf(ref.caseInsensitiveName2Id), List.copyOf(ref.store));
    }
}

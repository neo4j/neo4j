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
package org.neo4j.internal.batchimport.cache.idmapping;

import java.io.IOException;
import java.util.function.LongPredicate;
import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.neo4j.batchimport.api.PropertyValueLookup;
import org.neo4j.batchimport.api.input.Collector;
import org.neo4j.batchimport.api.input.Group;
import org.neo4j.batchimport.api.input.InputEntityVisitor;
import org.neo4j.internal.batchimport.cache.MemoryStatsVisitor;
import org.neo4j.internal.batchimport.cache.idmapping.cuckoo.KeyCollisionException;
import org.neo4j.internal.helpers.progress.ProgressMonitorFactory;
import org.neo4j.internal.id.IdGenerator;

/**
 * Maps input node ids as specified by data read into {@link InputEntityVisitor} into actual node ids.
 */
public interface IdMapper extends MemoryStatsVisitor.Visitable, AutoCloseable {
    long ID_NOT_FOUND = -1;
    int WORKER_ID_AGNOSTIC = -1;

    /**
     * Removes an existing mapping from {@code inputId} to {@code actualId}.
     * @param inputId an id of an unknown type, coming from input.
     * @param actualId the actual node id that the inputId maps to.
     * @param group the group that the {@code inputId} exists in.
     */
    void remove(Object inputId, long actualId, Group group);

    default void remove(Object[] inputIds, long[] actualIds, Group[] groups) {
        assert inputIds.length == actualIds.length && actualIds.length == groups.length;
        for (int i = 0; i < inputIds.length; i++) {
            remove(inputIds[i], actualIds[i], groups[i]);
        }
    }

    /**
     * @return whether a call to {@link #prepare(PropertyValueLookup, Collector, ProgressMonitorFactory, LongSet)} needs to commence after all calls to
     * {@link Setter#put(Object, long, Group)} and before any call to {@link Getter#get(Object, Group)}. I.e. whether all ids
     * need to be put before making any call to {@link Getter#get(Object, Group)}.
     */
    boolean needsPreparation();

    /**
     * After all mappings have been {@link Setter#put(Object, long, Group)} call this method to prepare for
     * {@link Getter#get(Object, Group)}.
     *
     * @param inputIdLookup can return input id of supplied node id. Used in the event of difficult collisions
     * so that more information have to be read from the input data again, data that normally isn't necessary
     * and hence discarded.
     * @param collector {@link Collector} for bad entries, such as duplicate node ids.
     * @param progressMonitorFactory reports preparation progress.
     * @param otherViolatingNodes entity IDs from other sources that are known to violate constraints.
     */
    void prepare(
            PropertyValueLookup inputIdLookup,
            Collector collector,
            ProgressMonitorFactory progressMonitorFactory,
            LongSet otherViolatingNodes);

    /**
     * @return a {@link Getter} for the current thread to do lookups in.
     */
    Getter newGetter(int workerId);

    Setter newSetter(int workerId);

    /**
     * A workerid agnostic newGetter.
     * @return a {@link Getter} for the current thread to do lookups in
     */
    default Getter newGetter() {
        throw new UnsupportedOperationException(
                "workerId agnostic idMapperGetters are only supported in SkidbladnirImporter");
    }

    /**
     * Releases all resources used by this {@link IdMapper}.
     */
    @Override
    void close();

    /**
     * Returns instance capable of returning memory usage estimation of given {@code numberOfNodes}.
     *
     * @param numberOfNodes number of nodes to calculate memory for.
     * @return instance capable of calculating memory usage for the given number of nodes.
     */
    MemoryStatsVisitor.Visitable memoryEstimation(long numberOfNodes);

    LongIterator leftOverDuplicateNodesIds();

    LongPredicate leftOverDuplicateNodesIdsPredicate();

    interface Getter extends AutoCloseable {
        /**
         * Returns an actual node id representing {@code inputId}.
         * For this call to work {@link #prepare(PropertyValueLookup, Collector, ProgressMonitorFactory, LongSet)} must have
         * been called after all calls to {@link Setter#put(Object, long, Group)} have been made,
         * iff {@link #needsPreparation()} returns {@code true}. Otherwise ids can be retrieved right after
         * {@link Setter#put(Object, long, Group) being put}
         *
         * @param inputId the input id to get the actual node id for.
         * @param group {@link Group} the given {@code inputId} must exist in, i.e. have been put with.
         * @return the actual node id previously specified by {@link Setter#put(Object, long, Group)}, or {@code -1} if not found.
         */
        long get(Object inputId, Group group);

        default long[] get(Object[] inputIds, Group[] groups) {
            assert inputIds.length == groups.length;
            long[] result = new long[groups.length];
            for (int i = 0; i < groups.length; i++) {
                result[i] = inputIds[i] == null ? ID_NOT_FOUND : get(inputIds[i], groups[i]);
            }
            return result;
        }

        @Override
        void close();
    }

    interface Setter extends AutoCloseable {
        /**
         * Maps an {@code inputId} to an actual node id.
         * @param inputId an id of an unknown type, coming from input.
         * @param actualId the actual node id that the inputId will represent.
         * @param group {@link Group} this input id will be added to. Used for handling input ids collisions
         * where multiple equal input ids might be added, as long as all input ids within a single group is unique.
         * Group ids are also passed into {@link Getter#get(Object, Group)}.
         * It is required that all input ids belonging to a specific group are put in sequence before putting any
         * input ids for another group.
         */
        void put(Object inputId, long actualId, Group group) throws KeyCollisionException;

        default boolean[] put(Object[] inputIds, long[] actualIds, Group[] groups) {
            assert inputIds.length == actualIds.length && actualIds.length == groups.length;
            boolean[] result = new boolean[inputIds.length];
            for (int i = 0; i < inputIds.length; i++) {
                try {
                    put(inputIds[i], actualIds[i], groups[i]);
                    result[i] = true;
                } catch (KeyCollisionException e) {
                    result[i] = false;
                }
            }
            return result;
        }

        @Override
        default void close() throws IOException {}
    }

    interface WithHighId extends IdMapper {
        /**
         * Returns one more than the highest actual ID that has been set.
         * This is equivalent to {@link IdGenerator#getHighId()}.
         * <p>
         * {@link #prepare(PropertyValueLookup, Collector, ProgressMonitorFactory, LongSet) prepare} needs to be called before this method, if
         * {@link #needsPreparation} returns {@code true}.
         */
        long getHighId();
    }
}

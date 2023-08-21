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
package org.neo4j.kernel.impl.locking.forseti;

import static org.neo4j.configuration.GraphDatabaseInternalSettings.lock_manager_verbose_deadlocks;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.SettingChangeListener;
import org.neo4j.kernel.impl.locking.LockManager;
import org.neo4j.lock.LockType;
import org.neo4j.lock.ResourceType;
import org.neo4j.time.SystemNanoClock;

/**
 * <h1>Forseti, the Nordic god of justice</h1>
 * <p/>
 * Forseti is a lock manager using the dreadlocks deadlock detection algorithm, which means
 * deadlock detection does not require complex RAG traversal and can be found in O(1).
 * <p/>
 * In the best case, Forseti acquires a lock in one CAS instruction, and scales linearly with the number of cores.
 * However, since it uses a shared-memory approach, it will most likely degrade in use cases where there is high
 * contention and a very large number of sockets running the database.
 * <p/>
 * As such, it is optimized for servers with up to, say, 16 cores across 2 sockets. Past that other strategies such
 * as centralized lock services using message passing may yield better results.
 * <p/>
 * <h2>Locking algorithm</h2>
 * <p/>
 * Forseti is used by acquiring clients, which act as agents on behalf of whoever wants to grab locks. The clients
 * have access to a central map of locks.
 * <p/>
 * To grab a lock, a client must insert itself into the holder list of the lock it wants. The lock may either be a
 * shared lock or an exclusive lock. In the case of a shared lock, the client simply appends itself to the holder list.
 * In the case of an exclusive lock, the client has it's own unique exclusive lock, which it must put into the lock map
 * using a CAS operation.
 * <p/>
 * Once the client is in the holder list, it has the lock.
 * <p/>
 * <h2>Deadlock detection</h2>
 * <p/>
 * Each Client maintains a waiting-for list, which by default always contains the client itself. This list indicates
 * which other clients are blocking our progress. By default, then, if client A is waiting for no-one, its waiting-for
 * list will contain only itself:
 * <p/>
 * A.waitlist = [A]
 * <p/>
 * Once the client is blocked by someone else, it will copy this someones entire wait list into it's own. Assuming A
 * becomes blocked by B, and B has a wait list of:
 * <p/>
 * B.waitlist = [B]
 * <p/>
 * Then A will modify is's wait list as:
 * <p/>
 * A.waitlist = [A] U [B] => [A,B]
 * <p/>
 * It will do this in a loop, continuously figuring out the union of wait lists for all clients it waits for. The magic
 * then happens whenever one of those clients become blocked on client A. Assuming client B now has to wait for A,
 * it will also perform a union of A's wait list (which is [A,B] at this point):
 * <p/>
 * B.waitlist = [B] U [A,B]
 * <p/>
 * As it performs this union, B will find itself in A's waiting list, and when it does, it has detected a deadlock.
 * <p/>
 * This algorithm always identifies real deadlocks, but it may also mistakenly identify a deadlock where there is none;
 * a false positive. For this reason, we have a secondary deadlock verification algorithm that only runs if the
 * algorithm above found what appears to be a deadlock.
 * <p/>
 * The secondary deadlock verification algorithm works like this: Whenever a lock client blocks to wait on a lock, the
 * lock is stored in the clients `waitsFor` field, and the field is cleared when the client unblocks. Since every lock
 * track their owners, we now have all the information we need to traverse the waiter/lock-holder dependency graph to
 * verify that a cycle really does exist.
 * <p/>
 * We first collect the owners of the lock that we are blocking upon. From there, we need to find a lock that one of
 * these lock-owners are waiting on, and have us amongst its owners. So to recap, we collect the immediate owners of
 * the lock that we are immediately blocked upon, then we collect the set of locks that they are waiting upon, and then
 * we collect the combined set of owners of <em>those</em> locks, and if we are amongst those, then we consider the
 * deadlock is real. If we are not amongst those owners, then we take another step out into the graph, collect the next
 * frontier of locks that are waited upon, and their owners, and then we check again in this new owner set. We continue
 * traversing the graph like this until we either find ourselves amongst the owners - a deadlock - or we run out of
 * locks that are being waited upon - no deadlock.
 * <p/>
 */
public class ForsetiLockManager implements LockManager {
    /** This is Forsetis internal lock API, which it uses to do deadlock detection. */
    interface Lock {
        /**
         * For each client currently holding this lock, copy their wait list into the given bitset.
         * This is how information on who is waiting for whom is propagated.
         */
        void copyHolderWaitListsInto(Set<ForsetiClient> waitList);

        /**
         * Check if anyone holding this lock is currently waiting for the specified client. This
         * check is performed continuously while a client waits for a lock - if the check ever
         * comes back positive, it means we've deadlocked, because we are waiting for someone
         * (the holder of the lock) who in turn is waiting for us (so they won't release the lock).
         *
         * @param client the client that is waiting to grab this lock
         * @return the client we've deadlocked with, or {@code null} if there is not currently a deadlock
         */
        ForsetiClient detectDeadlock(ForsetiClient client);

        /**
         * For introspection and error messages, this gives a (somewhat) human-readable description of who is waiting
         * for the lock.
         */
        String describeWaitList();

        /**
         * Collect the current owners of this lock into the given set. This is used for verifying that apparent
         * deadlocks really do involve circular wait dependencies.
         *
         * Note that the owner set may change while this method is running, and thus it is not guaranteed to reflect any
         * particular snapshot of the set of lock owners. Furthermore, the set may change arbitrarily after the method
         * returns, immediately rendering the result outdated.
         * @param owners The set into which to collect the current owners of this lock.
         */
        void collectOwners(Set<ForsetiClient> owners);

        boolean isOwnedBy(ForsetiClient client);

        LockType type();

        LongSet transactionIds();

        /**
         * Check if a lock is closed or not. A closed lock can not be locked, reused or be cause of deadlocks.
         */
        boolean isClosed();
    }

    private final Config config;
    private final SettingChangeListener<Boolean> verboseDeadlocksSettingListener;

    /** Pointers to lock maps, one array per resource type. */
    private final ConcurrentMap<Long, ForsetiLockManager.Lock>[] lockMaps;

    /** Reverse lookup resource types by id, used for introspection */
    private final ResourceType[] resourceTypes;

    /** Counter to keep internal client ids unique, important to be thread safe! */
    private final AtomicLong clientIds = new AtomicLong();

    private final SystemNanoClock clock;
    private volatile boolean verboseDeadlocks;
    private volatile boolean closed;

    @SuppressWarnings("unchecked")
    public ForsetiLockManager(Config config, SystemNanoClock clock, ResourceType... resourceTypes) {
        this.config = config;
        int maxResourceId = findMaxResourceId(resourceTypes);
        this.lockMaps = new ConcurrentMap[maxResourceId];
        this.resourceTypes = new ResourceType[maxResourceId];

        for (ResourceType type : resourceTypes) {
            this.lockMaps[type.typeId()] = new ConcurrentHashMap<>(16, 0.6f, 512);
            this.resourceTypes[type.typeId()] = type;
        }
        this.clock = clock;
        this.verboseDeadlocks = config.get(lock_manager_verbose_deadlocks);
        this.verboseDeadlocksSettingListener = (oldValue, newValue) -> verboseDeadlocks = newValue;
        config.addListener(lock_manager_verbose_deadlocks, verboseDeadlocksSettingListener);
    }

    /**
     * Create a new client to use to grab and release locks.
     */
    @Override
    public Client newClient() {
        if (closed) {
            throw new IllegalStateException(this + " already closed");
        }

        return new ForsetiClient(lockMaps, clock, verboseDeadlocks, clientIds.incrementAndGet());
    }

    @Override
    public void accept(Visitor out) {
        for (int i = 0; i < lockMaps.length; i++) {
            if (lockMaps[i] != null) {
                var resourceType = resourceTypes[i];
                for (Map.Entry<Long, Lock> entry : lockMaps[i].entrySet()) {
                    var lock = entry.getValue();
                    var description = lock.describeWaitList();
                    var transactionIds = lock.transactionIds();
                    int lockIdentityHashCode = System.identityHashCode(lock);
                    transactionIds.forEach(txId -> out.visit(
                            lock.type(), resourceType, txId, entry.getKey(), description, 0, lockIdentityHashCode));
                }
            }
        }
    }

    private static int findMaxResourceId(ResourceType[] resourceTypes) {
        int max = 0;
        for (ResourceType resourceType : resourceTypes) {
            max = Math.max(resourceType.typeId(), max);
        }
        return max + 1;
    }

    @Override
    public void close() {
        config.removeListener(lock_manager_verbose_deadlocks, verboseDeadlocksSettingListener);
        closed = true;
    }
}

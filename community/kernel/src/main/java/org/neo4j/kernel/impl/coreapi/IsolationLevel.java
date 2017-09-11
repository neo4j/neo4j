/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.kernel.impl.coreapi;

import java.util.EnumSet;

import static org.neo4j.kernel.impl.coreapi.IsolationLevel.Anomaly.CursorLostUpdate;
import static org.neo4j.kernel.impl.coreapi.IsolationLevel.Anomaly.DirtyRead;
import static org.neo4j.kernel.impl.coreapi.IsolationLevel.Anomaly.DirtyWrite;
import static org.neo4j.kernel.impl.coreapi.IsolationLevel.Anomaly.FuzzyRead;
import static org.neo4j.kernel.impl.coreapi.IsolationLevel.Anomaly.LostUpdate;
import static org.neo4j.kernel.impl.coreapi.IsolationLevel.Anomaly.ReadOnlySerialisationAnomaly;
import static org.neo4j.kernel.impl.coreapi.IsolationLevel.Anomaly.ReadSkew;
import static org.neo4j.kernel.impl.coreapi.IsolationLevel.Anomaly.UnstableIterator;
import static org.neo4j.kernel.impl.coreapi.IsolationLevel.Anomaly.WriteSkew;
import static org.neo4j.kernel.impl.coreapi.IsolationLevel.Supported.Supported;
import static org.neo4j.kernel.impl.coreapi.IsolationLevel.Supported.Unsupported;

/**
 * Following the refined definitions in A Critique of ANSI SQL Isolation Levels from Microsoft Research:
 * https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/tr-95-51.pdf
 * with additions from the A Read-Only Transaction Anomaly Under Snapshot Isolation paper by Fekete, O'Niel & O'Niel:
 * http://www.cs.umb.edu/~poneil/ROAnom.pdf
 */
public enum IsolationLevel
{
    /**
     * Prevents write conflicts at the record level, but allows transactions to observe the writes of other
     * transactions prior to their committing. This is the least safe isolation level that is possible to implement,
     * without risk of breaking internal database invariants, and features such as transaction rollback.
     */
    ReadUncommitted( Unsupported, EnumSet.of( DirtyWrite ) ),
    /**
     * Prevents dirty read and write at the record level, but otherwise allows all other anomalies to occur. This
     * includes anomalies that might not be immediately obvious, such as {@link Anomaly#CursorLostUpdate} and
     * {@link Anomaly#UnstableIterator}, due to the record level locking semantics.
     */
    ReadCommitted( Supported, EnumSet.of( DirtyWrite, DirtyRead ) ),
    /**
     * Like {@link IsolationLevel#ReadCommitted}, but additionally prevents {@link Anomaly#CursorLostUpdate} by holding
     * entity level shared locks while a database cursor is placed on an entity, such that the entity can be read and
     * then updated without losing any updates, provided the cursor is not moved or closed between the read and the
     * write.
     *
     * Note that this <em>does not</em> prevent the {@link Anomaly#UnstableIterator} anomaly. See the
     * {@link IsolationLevel#IteratorStability} isolation level for that.
     */
    CursorStability( Unsupported, EnumSet.of( DirtyWrite, DirtyRead, CursorLostUpdate ) ),
    /**
     * Like {@link IsolationLevel#ReadCommitted}, but additionally prevents {@link Anomaly#UnstableIterator} by holding
     * entity level shared locks while adjacent items of that entity are being iterated. Adjacent items are
     * relationships and properties for node entities, and properties for relationship entities.
     *
     * Note that this <em>does not</em> prevent the {@link Anomaly#CursorLostUpdate} anomaly. See the
     * {@link IsolationLevel#CursorStability} isolation level for that.
     */
    IteratorStability( Supported, EnumSet.of( DirtyWrite, DirtyRead, UnstableIterator ) ),
    /**
     * Completely isolates all forms of entity level reads and writes, including all forms of lost updates, and any
     * observation of writes from other concurrent transactions.
     *
     * Note that conflicts may be observed and resolved at the record level, so non-conflicting record level changes in
     * different transactions may produce entity level anomalies. This is important to keep in mind with the
     * {@link Anomaly#WriteSkew} anomaly, that is allowed under this isolation level.
     *
     * The key difference between this isolation level, and {@link IsolationLevel#RepeatableRead}, is that this level
     * prevents the {@link Anomaly#PhantomRead} anomaly where the results of two otherwise identical predicate reads
     * changes if a concurrent predicate matching write is committed in between the two. Whereas
     * {@link IsolationLevel#RepeatableRead} prevent the {@link Anomaly#WriteSkew} and
     * {@link Anomaly#ReadOnlySerialisationAnomaly} anomalies.
     */
    SnapshotIsolation( Unsupported, EnumSet.of(
            DirtyWrite, DirtyRead, CursorLostUpdate, LostUpdate, UnstableIterator, ReadSkew ) ),
    /**
     * Prevents all anomalies, except {@link Anomaly#PhantomRead}.
     */
    RepeatableRead( Unsupported, EnumSet.of(
            DirtyWrite, DirtyRead, CursorLostUpdate, LostUpdate, UnstableIterator, FuzzyRead, ReadSkew, WriteSkew,
            ReadOnlySerialisationAnomaly ) ),
    /**
     * This isolation level is by definition equivalent to a serial execution where no transactions overlap. Thus all
     * anomalies are prevented. Note, however, that no guarantee or indications are made about <em>which</em> serial
     * execution are particular set of transactions end up with. Transactions and their operations are in principle
     * allowed to reorder arbitrarily backwards and forwards in time, as long as some serial history is obtained.
     */
    Serializable( Unsupported, EnumSet.allOf( Anomaly.class ) );

    private final EnumSet<Anomaly> anomaliesPrevented;
    private final IsolationLevel.Supported supported;

    IsolationLevel( Supported supported, EnumSet<Anomaly> anomaliesPrevented )
    {
        this.supported = supported;
        this.anomaliesPrevented = anomaliesPrevented;
    }

    public boolean prevents( Anomaly anomaly )
    {
        return anomaliesPrevented.contains( anomaly );
    }

    public boolean allows( Anomaly anomaly )
    {
        return !prevents( anomaly );
    }

    public boolean isSupported()
    {
        return this.supported == Supported;
    }

    public enum Supported
    {
        Supported,
        Unsupported
    }

    /**
     * The set of anomalous behaviours used to define the various isolation levels.
     * <p>
     * Each behaviour is described as a history of actions that are impossible in serial execution, but can occur in
     * isolation levels lower than serializable.
     * <p>
     * Each history is described as a series of actions of type read, write, commit and abort, denoted by {@code r},
     * {@code w}, {@code c} and {@code a}, respectively. Each action is annotated with a number for the transaction in
     * which it occurs, possibly followed by the registers they act upon, in brackets.
     * <p>
     * For instance a read of {@code x} in {@code T1} is written {@code r1[x]}. If a register matches a relevant
     * predicate, then this is included in the bracket, e.g. {@code w1[x in P]} for a write to {@code x} that involves
     * the predicate {@code P}. Such writes include both deletes and updates such that {@code x} no longer matches
     * {@code P}, and creates and updates such that {@code x} come to match {@code P} where it previously did not.
     * <p>
     * The actions {@code rc} and {@code wc} are similar to the read and write actions, respectively, but used to
     * indicate the read and write are performed by a cursor assumed to be placed upon the given register.
     */
    public enum Anomaly
    {
        /**
         * Writes from two transactions overlap before either commits or rolls back.
         * <p>
         * Example: T1 modifies a register, which T2 then further modifies before T1 can commit or rollback. This allows
         * transactions to overwrite each others data before any of them commits or aborts. This can lead to
         * constraint violations, where transactions individually adhere to a constraint, but their spliced writes do
         * not. It can also make it impossible to abort a transaction, e.g. when the writes performed by a transaction
         * have been overwritten by another transaction. In such a case, there is no unambiguous before-image to roll
         * back to.
         * <p>
         * Concrete examples include two transactions writing to the same node property before either commits,
         * transactions creating nodes with the same id.
         * <p>
         * {@code P0: w1[x]…w2[x]…(c1 or a1)}
         */
        DirtyWrite,
        /**
         * Read observes a write from a transaction that has not yet committed or rolled back.
         * <p>
         * Example: T1 modifies a register, which T2 then observes before T1 commits or aborts. This allows T2 to make
         * decisions based on uncommitted data.
         * <p>
         * Concrete examples include a transaction seeing a property update from another transaction that has not
         * committed yet, seeing nodes and relationships created in transactions that have not committed yet.
         * <p>
         * {@code P1: w1[x]…r2[x]…(c1 or a1)}
         */
        DirtyRead,
        /**
         * A narrower case of the {@link Anomaly#LostUpdate} anomaly, in that the lost update happens after a cursor in
         * a transaction has moved to the register for reading it, and before the cursor performs a write on the
         * register. This also assumes that the cursor does not move away from the register between the read and the
         * write.
         * <p>
         * Example: A cursor is positioned on a register to read its value, and while keeping the cursor on the
         * register, a new value is computed and written back. However, in between the read and the write in this
         * transaction, another transaction was able to commit an update to the register, and this update is now lost
         * due to the write performed by the cursor.
         * <p>
         * {@code P4C: rc1[x]…w2[x]…wc1[x]…c1}
         */
        CursorLostUpdate,
        /**
         * A transaction reads a register, and then later writes to the register based on the earlier read. However, in
         * between the read and write, the register was modified by a concurrent transaction, whose update is now lost.
         * <p>
         * Example: T1 and T2 both read the value 100 out of register {@code A}. T2 wants to add 20 to the value, and
         * writes back 120 and commits. T1 then wants to add 30 to the value, and writes back 130 and commits. The
         * changes from T2 is lost, and the resulting value of the register is 20 less than it should be.
         * <p>
         * A concrete example is two transactions reading a node property, and one committing an update to the property
         * followed by the other transaction also updating the property, without seeing the update from the first
         * transaction.
         * <p>
         * {@code P4: r1[x]…w2[x]…w1[x]…c1}
         */
        LostUpdate,
        /**
         * A transaction queries for a predicate. As the results of this query is being processed, the result is
         * changed by a concurrent transaction that performs a write on a register matching the predicate.
         * <p>
         * A predicate can be thought of as the set of registers matching the predicate. A transaction reading a
         * predicate in this formalism can thus be broken down to reading the registers in the set one by one.
         * Concurrent transactions can then add or remove members of the set, as they are being processed, leading the
         * read transaction to observe a set that matches no before or after state of any write transaction.
         * <p>
         * This is a narrower case of {@link Anomaly#PhantomRead}. However, if the predicate is ignored and understood
         * purely as a set of registers intended to be read together, under some constraint, then this can also be
         * construed as a {@link Anomaly#ReadSkew} case.
         * <p>
         * A concrete example is a node that has one relationship, and a transaction deletes the relationship and
         * creates another, while a concurrent read transaction observes that the node has either two or zero
         * relationships.
         * <pre><code>
         * UIA: r1[P={a,b}]…w2[a in P]…r1[a]…(c1 or a1)
         * UIB: r1[P={b}]…r1[b]…w2[a in P]…(c1 or a1)
         * </code></pre>
         */
        UnstableIterator,
        /**
         * Also known as <em>Non-Repeatable Read</em>. After reading a register, its value changes due to writes to the
         * register being committed in concurrent transactions.
         * <p>
         * Example: The registers {@code A} and {@code B} have the initial values 50 and 0, respectively. T1 reads 50
         * from register {@code A}. T2 subtracts 50 from register {@code A} and adds it to register {@code B}, and
         * commits. T1 then reads 50 from register {@code B}, and computes an incorrect sum of 100.
         * <p>
         * Note how this anomaly does not require repeated reads of the same registers to manifest.
         * <p>
         * {@code P2: r1[x]…w2[x]…(c1 or a1)}
         */
        FuzzyRead,
        /**
         * A <em>predicate read</em> in one transaction is later invalidated when another transaction commits a write on
         * a register that match the predicate in its before or after image.
         * <p>
         * Example: T1 performs a search. T2 inserts a register that would have matched the search performed by T1. T2
         * then increments the value in a different register that represents the count of registers matching the search,
         * and then commits. T1 then reads the count register as a check, and finds a discrepancy.
         * <p>
         * {@code P3: r1[P]…w2[y in P]…(c1 or a1)}
         */
        PhantomRead,
        /**
         * If two registers are always updated together, or together form a constraint, then writes to both could
         * interject in between the reads of the two, resulting in an inconsistent view.
         * <p>
         * Example: Suppose there is a constraint that values are always unique, and that the registers {@code A} and
         * {@code B} have the values 1 and 2, respectively. T1 can read {@code A} and observe the value 1. Then T2
         * commit writes that change {@code A} to 2, and {@code B} to 1. And then T1 read {@code B} and observe the
         * value 1 again. T1 has observed that {@code A} and {@code B} both have the value 1, which violates the
         * uniqueness constraint.
         * <p>
         * {@code A5A: r1[x]…w2[x]…w2[y]…c2…r1[y]…(c1 or a1)}
         */
        ReadSkew,
        /**
         * Two writes, in each their own transaction, which individually violate no invariant, but taken together do
         * violate an invariant.
         * <p>
         * Example: Suppose there is an invariant that the sum of the registers {@code A} and {@code B} is positive,
         * and that they both have the initial value 1. T1 and T2 both read {@code A} and {@code B}, and determine
         * that 1 could be subtracted from either, but not both, of the registers. T1 updates {@code A} to 0, and T2
         * updates {@code B} to 0. The positive sum invariant is now violated.
         * <p>
         * {@code A5B: r1[x]…r2[y]…w1[y]…w2[x]…(c1 and c2 occur)}
         */
        WriteSkew,
        /**
         * A read-only transaction observes a different serial order of write transactions than their actual order,
         * and can thus conclude that future or concurrent write transactions violate invariants when they in fact do
         * not.
         * <p>
         * Example: Say we have the registers {@code X} and {@code Y} that initially are both zero, with the rule that
         * any write that causes their sum to become negative will incur a penalty of 1 to be subtracted from {@code Y}.
         * The write transactions T1 and T2 both start by observing that both registers are zero. T1 writes 20 to
         * {@code Y}. The read transaction T3 then starts, observes the values 0 and 20 for the registers {@code X} and
         * {@code Y}, respectively, and concludes that a withdrawal initiated earlier will not incur any penalty. T3
         * then commits, having done its read. T2 then wishes to withdraw 10 from {@code X}, but remembers that with its
         * initial read of zero from both registers, it will cause the sum to become negative. Therefor 11 is ultimately
         * subtracted from {@code X}, which is contrary to the expectations of T3.
         */
        ReadOnlySerialisationAnomaly
    }
}

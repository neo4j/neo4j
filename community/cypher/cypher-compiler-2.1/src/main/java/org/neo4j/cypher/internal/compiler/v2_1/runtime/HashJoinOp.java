/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.runtime;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class HashJoinOp implements Operator {
    private final int joinKeyId;
    private final int[] lhsTailLongIdx;
    private final int[] lhsTailObjectIdx;
    private final Operator lhs;
    private final Operator rhs;
    private final Registers registers;
    private final Map<Long, List<Registers>> bucket = new HashMap<>();
    private int bucketPos = 0;
    private List<Registers> currentBucketEntry = null;

    public HashJoinOp(int joinKeyId, int[] lhsTailLongIdx, int[] lhsTailObjectIdx, Operator lhs, Operator rhs, Registers registers) {
        this.joinKeyId = joinKeyId;
        this.lhsTailLongIdx = lhsTailLongIdx;
        this.lhsTailObjectIdx = lhsTailObjectIdx;
        this.lhs = lhs;
        this.rhs = rhs;
        this.registers = registers;

        fillHashBucket();
    }

    @Override
    public void open() {
        lhs.open();
        rhs.open();
    }

    @Override
    public boolean next() {
        while (currentBucketEntry == null || bucketPos >= currentBucketEntry.size()) {
            // If we've emptied our rhs, we're done here
            if (!rhs.next()) {
                return false;
            }

            // let's see if we find a match
            produceMatchIfPossible();
        }

        // We've found a match! Let's copy the data over.
        restoreFromTailEntry();

        return true;
    }

    private void produceMatchIfPossible() {
        long key = registers.getLongRegister(joinKeyId);
        currentBucketEntry = bucket.get(key);
        bucketPos = 0;
    }

    @Override
    public void close() {
        rhs.close();
        lhs.close();
    }

    private void fillHashBucket() {
        while (lhs.next()) {
            long key = registers.getLongRegister(joinKeyId);
            List<Registers> objects = getTailEntriesForId(key);
            Registers tailEntry = copyToTailEntry();
            objects.add(tailEntry);
        }
    }

    private List<Registers> getTailEntriesForId(long key) {
        List<Registers> objects = bucket.get(key);
        if (objects == null) {
            objects = new LinkedList<>();
            bucket.put(key, objects);
        }
        return objects;
    }

    private void restoreFromTailEntry() {
        int idx = bucketPos++;
        Registers from = currentBucketEntry.get(idx);
        Registers to = registers;

        for (int i = 0; i < lhsTailLongIdx.length; i++) {
            long temp = from.getLongRegister(i);
            to.setLongRegister(lhsTailLongIdx[i], temp);
        }

        for (int i = 0; i < lhsTailObjectIdx.length; i++) {
            Object temp = from.getObjectRegister(i);
            to.setObjectRegister(lhsTailObjectIdx[i], temp);
        }
    }

    private Registers copyToTailEntry() {
        Registers tailEntry = new MapRegisters();

        for (int i = 0; i < lhsTailLongIdx.length; i++) {
            long temp = registers.getLongRegister(lhsTailLongIdx[i]);
            tailEntry.setLongRegister(i, temp);
        }

        for (int i = 0; i < lhsTailObjectIdx.length; i++) {
            Object temp = registers.getObjectRegister(lhsTailLongIdx[i]);
            tailEntry.setObjectRegister(i, temp);
        }

        return tailEntry;
    }
}

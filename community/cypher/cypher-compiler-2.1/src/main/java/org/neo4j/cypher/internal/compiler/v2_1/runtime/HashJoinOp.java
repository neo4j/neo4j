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

import org.neo4j.cypher.internal.compiler.v2_1.spi.StatementContext;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class HashJoinOp implements Operator {
    private final StatementContext ctx;
    private EntityRegister joinRegister;
    private EntityRegister[] lhsTailEntityRegister;
    private ObjectRegister[] lhsTailObjectRegister;
    private final Operator lhs;
    private final Operator rhs;
    private final Map<Long, List<Registers>> bucket = new HashMap<>();
    private int bucketPos = 0;
    private List<Registers> currentBucketEntry = null;

    public HashJoinOp(StatementContext ctx,
                      EntityRegister joinNode,
                      EntityRegister[] lhsTailEntityRegisters,
                      ObjectRegister[] lhsTailObjectRegisters,
                      Operator lhs,
                      Operator rhs)
    {
        this.ctx = ctx;
        this.joinRegister = joinNode;
        this.lhsTailEntityRegister = lhsTailEntityRegisters;
        this.lhsTailObjectRegister = lhsTailObjectRegisters;

        this.lhs = lhs;
        this.rhs = rhs;

        fillHashBucket();
    }

    @Override
    public void open() {
        lhs.open();
        rhs.open();
    }

    @Override
    public boolean next() {
        while (currentBucketEntry == null || bucketPos >= currentBucketEntry.size())
        {
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
        long key = joinRegister.getEntity();
        currentBucketEntry = bucket.get(key);
        bucketPos = 0;
    }

    @Override
    public void close() {
        rhs.close();
        lhs.close();
    }

    private void fillHashBucket()
    {
        while (lhs.next())
        {
            long key = joinRegister.getEntity();
            List<Registers> objects = getTailEntriesForId(key);
            Registers tailEntry = copyToTailEntry();
            objects.add(tailEntry);
        }
    }

    private List<Registers> getTailEntriesForId(long key)
    {
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

        for (int i = 0; i < lhsTailEntityRegister.length; i++)
        {
            lhsTailEntityRegister[i].copyFrom( from );
        }

        for (int i = 0; i < lhsTailObjectRegister.length; i++)
        {
            lhsTailObjectRegister[i].copyFrom( from );
        }
    }

    private Registers copyToTailEntry()
    {
        // TODO: Make configurable / avoid allocation
        Registers tailEntry = new MapRegisters();

        for (int i = 0; i < lhsTailEntityRegister.length; i++)
        {
            lhsTailEntityRegister[i].copyTo( tailEntry );
        }

        for (int i = 0; i < lhsTailObjectRegister.length; i++)
        {
            lhsTailObjectRegister[i].copyTo( tailEntry );
        }

        return tailEntry;
    }
}

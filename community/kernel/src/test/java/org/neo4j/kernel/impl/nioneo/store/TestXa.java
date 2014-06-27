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
package org.neo4j.kernel.impl.nioneo.store;

import org.junit.Test;
// TODO 2.2-future some tests here may be a good idea to reimplement.

public class TestXa
{
    @Test
    public void testLogicalLogPrepared() throws Exception
    {
        // Recovers prepared but not committed transaction
    }

    @Test
    public void testLogicalLogPreparedPropertyBlocks() throws Exception
    {
        // Recovers prepared but not committed transaction which contains property blocks
    }

    @Test
    public void makeSureRecordsAreCreated() throws Exception
    {
    	// not sure
    }

    @Test
    public void testDynamicRecordsInLog() throws Exception
    {
    	// recovery of dynamic records works
    }

    @Test
    public void testLogicalLogPrePrepared() throws Exception
    {
        // ensures that not prepared transaction is not recovered
    }

    @Test
    public void testBrokenNodeCommand() throws Exception
    {
        // Ensures that transaction with truncated node command is not recovered
    }

    @Test
    public void testBrokenPrepare() throws Exception
    {
        // Ensures that transaction with truncated prepared entry is not recovered
    }

    @Test
    public void testBrokenDone() throws Exception
    {
        // ensures that transaction with truncated done entry is recovered
    }

    @Test
    public void testLogicalLogRotation() throws Exception
    {
        // ensures that logical log rotation increases version
    }
}

/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.kernel.lifecycle;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.Collections;

import org.neo4j.test.rule.fs.DefaultFileSystemRule;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

public class GitHub1304Test
{
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();
    @Rule
    public DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();

    @Test
    public void givenBatchInserterWhenArrayPropertyUpdated4TimesThenShouldNotFail() throws Exception
    {
        BatchInserter batchInserter = BatchInserters.inserter( folder.getRoot().getAbsoluteFile(), fileSystemRule.get() );

        long nodeId = batchInserter.createNode( Collections.emptyMap() );

        for ( int i = 0; i < 4; i++ )
        {
            batchInserter.setNodeProperty( nodeId, "array", new byte[]{2, 3, 98, 1, 43, 50, 3, 33, 51, 55, 116, 16, 23,
                    56, 9, -10, 1, 3, 1, 1, 1, 1, 1, 1, 1, 1, 1} );
        }

        batchInserter.getNodeProperties( nodeId );   //fails here
        batchInserter.shutdown();
    }
}

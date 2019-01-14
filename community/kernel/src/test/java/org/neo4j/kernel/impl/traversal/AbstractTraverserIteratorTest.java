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
package org.neo4j.kernel.impl.traversal;

import org.junit.Test;

import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Resource;
import org.neo4j.graphdb.traversal.BranchState;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.TraversalBranch;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class AbstractTraverserIteratorTest
{
    @Test
    public void shouldCloseResourceOnce()
    {
        AbstractTraverserIterator iter = new AbstractTraverserIterator( new AssertOneClose() )
        {

            @Override
            protected Path fetchNextOrNull()
            {
                return null;
            }

            @Override
            public boolean isUniqueFirst( TraversalBranch branch )
            {
                return false;
            }

            @Override
            public boolean isUnique( TraversalBranch branch )
            {
                return false;
            }

            @Override
            public <STATE> Evaluation evaluate( TraversalBranch branch, BranchState<STATE> state )
            {
                return null;
            }
        };

        iter.close();
        iter.close(); // should not fail
    }

    private static class AssertOneClose implements Resource
    {
        boolean isClosed;

        @Override
        public void close()
        {
            assertThat("resource is closed", isClosed, equalTo(false));
            isClosed = true;
        }
    }
}

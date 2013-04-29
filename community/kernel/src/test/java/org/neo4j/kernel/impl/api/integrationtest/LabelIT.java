/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.api.integrationtest;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

import java.util.Iterator;

import org.junit.Test;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.impl.core.LabelToken;

public class LabelIT extends KernelIntegrationTest
{
    @Test
    public void shouldListAllLabels() throws Exception
    {
        // given
        newTransaction();
        long label1Id = statement.getOrCreateLabelId( "label1" );
        long label2Id = statement.getOrCreateLabelId( "label2" );

        // when
        Iterator<LabelToken> labelIdsBeforeCommit = statement.listLabels();

        // then
        assertEquals( asList( new LabelToken( "label1", (int) label1Id ), new LabelToken( "label2", (int) label2Id ) ),
                IteratorUtil.asCollection( labelIdsBeforeCommit ) );

        // when
        commit();
        newTransaction();
        Iterator<LabelToken> labelIdsAfterCommit = statement.listLabels();

        // then
        assertEquals( asList( new LabelToken( "label1", (int) label1Id ), new LabelToken( "label2", (int) label2Id ) ),
                IteratorUtil.asCollection( labelIdsAfterCommit ) );
    }
}

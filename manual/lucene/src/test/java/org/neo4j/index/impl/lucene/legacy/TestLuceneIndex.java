/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.index.impl.lucene.legacy;

import org.junit.Test;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.index.Index;
import org.neo4j.helpers.collection.MapUtil;

import static org.neo4j.index.Neo4jTestCase.assertContains;

public class TestLuceneIndex extends AbstractLuceneIndexTest {

    @Test
    public void exactIndexWithCaseInsensitiveWithBetterConfig() throws Exception
    {
        // START SNIPPET: exact-case-insensitive
        Index<Node> index = graphDb.index().forNodes( "exact-case-insensitive",
                MapUtil.stringMap( "type", "exact", "to_lower_case", "true" ) );
        Node node = graphDb.createNode();
        index.add( node, "name", "Thomas Anderson" );
        assertContains( index.query( "name", "\"Thomas Anderson\"" ), node );
        assertContains( index.query( "name", "\"thoMas ANDerson\"" ), node );
        // END SNIPPET: exact-case-insensitive
        restartTx();
        assertContains( index.query( "name", "\"Thomas Anderson\"" ), node );
        assertContains( index.query( "name", "\"thoMas ANDerson\"" ), node );
    }

}

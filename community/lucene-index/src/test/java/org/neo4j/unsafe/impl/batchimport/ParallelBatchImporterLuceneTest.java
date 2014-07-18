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
package org.neo4j.unsafe.impl.batchimport;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.tooling.GlobalGraphOperations;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import static org.neo4j.helpers.collection.IteratorUtil.count;

public class ParallelBatchImporterLuceneTest extends ParallelBatchImporterTest
{
    @Override
    protected void verifyData( int nodeCount, GraphDatabaseService db )
    {
        super.verifyData( nodeCount, db );

        GlobalGraphOperations globalOps = GlobalGraphOperations.at( db );
        // The label scan store should find all nodes since they all have the same labels
        Label firstLabel = DynamicLabel.label( LABELS[0] );
        ResourceIterable<Node> allNodesWithLabel =
                globalOps.getAllNodesWithLabel( firstLabel );
        assertThat( count( allNodesWithLabel ), is( nodeCount ) );

        // All nodes also have the same age=10 property, so we should again find them all
        ResourceIterable<Node> foundNodes =
                db.findNodesByLabelAndProperty( firstLabel, "age", 10 );
        assertThat( count( foundNodes ), is( nodeCount ) );
    }
}

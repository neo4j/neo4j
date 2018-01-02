/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.visualization.graphviz;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ReturnableEvaluator;
import org.neo4j.graphdb.StopEvaluator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.Traverser.Order;
import org.neo4j.test.DatabaseRule;
import org.neo4j.test.ImpermanentDatabaseRule;
import org.neo4j.walk.Walker;

public class TestNewGraphvizWriter
{
	enum type implements RelationshipType
	{
		KNOWS, WORKS_FOR
	}

	public final @Rule DatabaseRule dbRule = new ImpermanentDatabaseRule();

	@Test
	public void testSimpleGraph() throws Exception
	{
	    GraphDatabaseService neo = dbRule.getGraphDatabaseService();
		try ( Transaction tx = neo.beginTx() )
		{
			final Node emil = neo.createNode();
			emil.setProperty( "name", "Emil Eifrém" );
			emil.setProperty( "age", 30 );
			final Node tobias = neo.createNode();
			tobias.setProperty( "name", "Tobias \"thobe\" Ivarsson" );
			tobias.setProperty( "age", 23 );
			tobias.setProperty( "hours", new int[] { 10, 10, 4, 4, 0 } );
			final Node johan = neo.createNode();
			johan.setProperty( "!<>)", "!<>)" );
            johan.setProperty( "name", "!<>Johan '\\n00b' !<>Svensson" );
			final Relationship emilKNOWStobias = emil.createRelationshipTo(
			    tobias, type.KNOWS );
			emilKNOWStobias.setProperty( "since", "2003-08-17" );
			final Relationship johanKNOWSemil = johan.createRelationshipTo(
			    emil, type.KNOWS );
			final Relationship tobiasKNOWSjohan = tobias.createRelationshipTo(
			    johan, type.KNOWS );
			final Relationship tobiasWORKS_FORemil = tobias
			    .createRelationshipTo( emil, type.WORKS_FOR );
			OutputStream out = new ByteArrayOutputStream();
			GraphvizWriter writer = new GraphvizWriter();
            writer.emit( out, Walker.crosscut( emil.traverse( Order.DEPTH_FIRST,
                    StopEvaluator.END_OF_GRAPH, ReturnableEvaluator.ALL, type.KNOWS,
                    Direction.BOTH, type.WORKS_FOR, Direction.BOTH ), type.KNOWS, type.WORKS_FOR ) );
			tx.success();
			out.toString();
		}
	}
}

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
package org.neo4j.kernel.impl.api;

import org.junit.After;
import org.junit.Test;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;

import static org.junit.Assert.assertTrue;

import static org.neo4j.graphdb.DynamicLabel.label;

public class LabelRecoveryTest
{
    /**
     * Reading a node command might leave a node record which referred to
     * labels in one or more dynamic records as marked as heavy even if that
     * node already had references to dynamic records, changed in a transaction,
     * but had no labels on that node changed within that same transaction.
     * Now defensively only marks as heavy if there were one or more dynamic
     * records provided when providing the record object with the label field
     * value. This would give the opportunity to load the dynamic records the
     * next time that record would be ensured heavy.
     */
    @Test
    public void shouldRecoverNodeWithDynamicLabelRecords() throws Exception
    {
        // GIVEN
        database = new TestGraphDatabaseFactory().setFileSystem( fs ).newImpermanentDatabase();
        Node node;
        Label[] labels = new Label[] { label( "a" ),
                label( "b" ),
                label( "c" ),
                label( "d" ),
                label( "e" ),
                label( "f" ),
                label( "g" ),
                label( "h" ),
                label( "i" ),
                label( "j" ),
                label( "k" ) };
        try ( Transaction tx = database.beginTx() )
        {
            node = database.createNode( labels );
            tx.success();
        }

        // WHEN
        try ( Transaction tx = database.beginTx() )
        {
            node.setProperty( "prop", "value" );
            tx.success();
        }
        EphemeralFileSystemAbstraction snapshot = fs.snapshot();
        database.shutdown();
        database = new TestGraphDatabaseFactory().setFileSystem( snapshot ).newImpermanentDatabase();

        // THEN
        try ( Transaction ignored = database.beginTx() )
        {
            node = database.getNodeById( node.getId() );
            for ( Label label : labels )
            {
                assertTrue( node.hasLabel( label ) );
            }
        }
    }

    @After
    public void tearDown()
    {
        if ( database != null )
        {
            database.shutdown();
        }
        fs.shutdown();
    }

    public final EphemeralFileSystemAbstraction fs = new EphemeralFileSystemAbstraction();
    private GraphDatabaseService database;
}

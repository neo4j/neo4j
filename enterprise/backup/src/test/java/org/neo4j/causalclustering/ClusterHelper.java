/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.causalclustering;

import org.neo4j.causalclustering.core.CoreGraphDatabase;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.DbRepresentation;

public class ClusterHelper
{
    public static final Label label = Label.label( "any_label" );
    public static final String PROP_NAME = "name";
    public static final String PROP_RANDOM = "random";

    /**
     * Used by non-cc
     * @param db
     * @return
     */
    public static DbRepresentation createSomeData( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            createSomeData(db, tx);
        }
        return DbRepresentation.of( db );
    }

    /**
     * Used by cc
     * @param db
     * @param tx
     */
    public static void createSomeData( GraphDatabaseService db, Transaction tx )
    {
        Node node = db.createNode();
        node.setProperty( PROP_NAME, "Neo" );
        node.setProperty( PROP_RANDOM, Math.random() * 10000 );
        db.createNode().createRelationshipTo( node, RelationshipType.withName( "KNOWS" ) );
        tx.success();
    }

    public static void createIndexes( CoreGraphDatabase db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().indexFor( label ).on( PROP_NAME ).on( PROP_RANDOM ).create();
            tx.success();
        }
    }

}

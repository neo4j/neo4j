/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering.helpers;

import java.util.function.Supplier;

import org.neo4j.causalclustering.core.CoreGraphDatabase;
import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.discovery.CoreClusterMember;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.Pair;

import static org.neo4j.helpers.collection.Iterables.count;

public class DataCreator
{
    private DataCreator()
    {
    }

    public static CoreClusterMember createLabelledNodesWithProperty( Cluster cluster, int numberOfNodes,
            Label label, Supplier<Pair<String,Object>> propertyPair ) throws Exception
    {
        CoreClusterMember last = null;
        for ( int i = 0; i < numberOfNodes; i++ )
        {
            last = cluster.coreTx( ( db, tx ) ->
            {
                Node node = db.createNode( label );
                node.setProperty( propertyPair.get().first(), propertyPair.get().other() );
                tx.success();
            } );
        }
        return last;
    }

    public static CoreClusterMember createEmptyNodes( Cluster cluster, int numberOfNodes ) throws Exception
    {
        CoreClusterMember last = null;
        for ( int i = 0; i < numberOfNodes; i++ )
        {
            last = cluster.coreTx( ( db, tx ) ->
            {
                db.createNode();
                tx.success();
            } );
        }
        return last;
    }

    public static long countNodes( CoreClusterMember member )
    {
        CoreGraphDatabase db = member.database();
        long count;
        try ( Transaction tx = db.beginTx() )
        {
            count = count( db.getAllNodes() );
            tx.success();
        }
        return count;
    }
}

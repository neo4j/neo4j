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
package org.neo4j.kernel.impl.store;

import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.api.CountsRecordState;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.transaction.state.NeoStoreProvider;

import static org.neo4j.kernel.impl.store.NodeLabelsField.parseLabelsField;

public class CountsComputer
{
    public static CountsRecordState computeCounts( GraphDatabaseAPI api )
    {
        return computeCounts( api.getDependencyResolver().resolveDependency( NeoStoreProvider.class ).evaluate() );
    }

    public static CountsRecordState computeCounts( NeoStore stores )
    {
        return computeCounts( stores.getNodeStore(), stores.getRelationshipStore() );
    }

    public static CountsRecordState computeCounts( NodeStore nodeStore, RelationshipStore relationshipStore )
    {
        final CountsRecordState result = new CountsRecordState();
        new CountsComputer( nodeStore, relationshipStore ).update( result );
        return result;
    }

    private final NodeStore nodes;
    private final RelationshipStore relationships;

    public CountsComputer( NodeStore nodes, RelationshipStore relationships )
    {
        this.nodes = nodes;
        this.relationships = relationships;
    }

    public void update( CountsRecordState target )
    {
        // count nodes
        for ( long id = 0, highId = nodes.getHighId(); id <= highId; id++ )
        {
            NodeRecord record = nodes.forceGetRecord( id );
            if ( record.inUse() )
            {
                target.addNode( labels( record ) );
            }
        }
        // count relationships
        for ( long id = 0, highId = relationships.getHighId(); id <= highId; id++ )
        {
            RelationshipRecord record = relationships.forceGetRecord( id );
            if ( record.inUse() )
            {
                long[] startLabels = labels( nodes.forceGetRecord( record.getFirstNode() ) );
                long[] endLabels = labels( nodes.forceGetRecord( record.getSecondNode() ) );
                target.addRelationship( startLabels, record.getType(), endLabels );
            }
        }
    }

    private long[] labels( NodeRecord node )
    {
        return parseLabelsField( node ).get( nodes );
    }
}

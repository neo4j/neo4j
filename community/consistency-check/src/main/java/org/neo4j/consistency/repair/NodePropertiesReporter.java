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
package org.neo4j.consistency.repair;

import static java.lang.String.format;

import java.io.PrintWriter;
import java.util.HashSet;
import java.util.Set;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;

public class NodePropertiesReporter
{
    private final GraphDatabaseService database;

    public NodePropertiesReporter( GraphDatabaseService database )
    {
        this.database = database;
    }

    public void reportNodeProperties( PrintWriter writer, RecordSet<RelationshipRecord> relationshipRecords )
    {
        Set<Long> nodeIds = new HashSet<Long>();

        for ( RelationshipRecord relationshipRecord : relationshipRecords )
        {
            nodeIds.add( relationshipRecord.getFirstNode() );
            nodeIds.add( relationshipRecord.getSecondNode() );
        }

        for ( Long nodeId : nodeIds )
        {
            reportNodeProperties( writer, nodeId );
        }
    }

    private void reportNodeProperties( PrintWriter writer, Long nodeId )
    {
        try
        {
            Node node = database.getNodeById( nodeId );

            writer.println( String.format( "Properties for node %d", nodeId ) );
            for ( String propertyKey : node.getPropertyKeys() )
            {
                writer.println( String.format( "    %s = %s", propertyKey, node.getProperty( propertyKey ) ) );
            }
        }
        catch ( Exception e )
        {
            writer.println( format( "Failed to report properties for node %d:", nodeId ) );
            e.printStackTrace( writer );
        }
    }
}

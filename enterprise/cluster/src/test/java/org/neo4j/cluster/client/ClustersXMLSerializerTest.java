/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cluster.client;

import javax.xml.parsers.DocumentBuilderFactory;

import org.junit.Test;
import org.w3c.dom.Document;

import static org.junit.Assert.assertEquals;

public class ClustersXMLSerializerTest
{
    @Test
    public void testWriteRead() throws Exception
    {
        ClustersXMLSerializer serializer = new ClustersXMLSerializer( DocumentBuilderFactory.newInstance()
                .newDocumentBuilder() );

        Clusters clusters = new Clusters();
        Clusters.Cluster cluster = new Clusters.Cluster( "default" );
        clusters.getClusters().add( cluster );

        cluster.getMembers().add( new Clusters.Member( "localhost:8001" ) );
        cluster.getMembers().add( new Clusters.Member( "localhost:8002" ) );

        Document doc = serializer.write( clusters );

        Clusters clusters2 = serializer.read( doc );

        assertEquals( clusters, clusters2 );
    }
}

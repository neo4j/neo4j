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

import javax.xml.parsers.DocumentBuilder;

import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * Serialize to and from the cluster discovery XML format.
 */
public class ClustersXMLSerializer
{
    private DocumentBuilder documentBuilder;

    public ClustersXMLSerializer( DocumentBuilder documentBuilder )
    {
        this.documentBuilder = documentBuilder;
    }

    public Clusters read( Document clustersXml )
    {
        Clusters clusters = new Clusters();

        NodeList clustersList = clustersXml.getElementsByTagName( "cluster" );
        for ( int i = 0; i < clustersList.getLength(); i++ )
        {
            Node clusterNode = clustersList.item( i );
            Clusters.Cluster cluster = new Clusters.Cluster( clusterNode.getAttributes().getNamedItem( "name" )
                    .getTextContent() );

            NodeList nodeList = clusterNode.getChildNodes();
            for ( int j = 0; j < nodeList.getLength(); j++ )
            {
                Node nodeNode = nodeList.item( j );
                String host = nodeNode.getTextContent().trim();
                if ( !host.equals( "" ) )
                {
                    Clusters.Member member = new Clusters.Member( nodeNode.getTextContent().trim() );
                    cluster.getMembers().add( member );
                }
            }

            clusters.getClusters().add( cluster );
        }

        return clusters;
    }

    public Document write( Clusters clusters )
    {
        Document doc = documentBuilder.newDocument();
        Node clustersNode = doc.createElement( "clusters" );
        doc.appendChild( clustersNode );
        for ( int i = 0; i < clusters.getClusters().size(); i++ )
        {
            Clusters.Cluster cluster = clusters.getClusters().get( i );

            Node clusterNode = doc.createElement( "cluster" );
            Attr name = doc.createAttribute( "name" );
            name.setValue( cluster.getName() );
            clusterNode.getAttributes().setNamedItem( name );
            clustersNode.appendChild( clusterNode );

            for ( Clusters.Member member : cluster.getMembers() )
            {
                Node nodeNode = doc.createElement( "member" );
                nodeNode.setTextContent( member.getHost() );
                clusterNode.appendChild( nodeNode );
            }
        }

        return doc;
    }
}

/**
 * Copyright (c) 2002-2010 "Neo Technology,"
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

package org.neo4j.server.webadmin.parser;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.events.XMLEvent;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;

/**
 * Near-clone of blueprints GraphMLReader. This is neo4j-specific though,
 * manually handling transactions rather than doing one transaction per
 * operation.
 * 
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@SuppressWarnings( "restriction" )
public class GraphMLReader
{

    private static final int OPERATIONS_PER_COMMIT = 5000;

    public static void inputGraph( final GraphDatabaseService graph,
            final InputStream graphMLInputStream ) throws XMLStreamException
    {

        XMLInputFactory inputFactory = XMLInputFactory.newInstance();
        XMLStreamReader reader = inputFactory.createXMLStreamReader( graphMLInputStream );

        Map<String, String> keyIdMap = new HashMap<String, String>();
        Map<String, String> keyTypesMaps = new HashMap<String, String>();
        Map<String, Object> vertexIdMap = new HashMap<String, Object>();

        Node currentVertex = null;
        Relationship currentEdge = null;

        Transaction tx = graph.beginTx();
        int ops = 0;
        try
        {
            while ( reader.hasNext() )
            {

                if ( ( ops++ ) % OPERATIONS_PER_COMMIT == 0 )
                {
                    tx.success();
                    tx.finish();
                    tx = graph.beginTx();
                }

                Integer eventType = reader.next();
                if ( eventType.equals( XMLEvent.START_ELEMENT ) )
                {
                    String elementName = reader.getName().getLocalPart();
                    if ( elementName.equals( GraphMLTokens.KEY ) )
                    {
                        String id = reader.getAttributeValue( null,
                                GraphMLTokens.ID );
                        String attributeName = reader.getAttributeValue( null,
                                GraphMLTokens.ATTR_NAME );
                        String attributeType = reader.getAttributeValue( null,
                                GraphMLTokens.ATTR_TYPE );
                        keyIdMap.put( id, attributeName );
                        keyTypesMaps.put( attributeName, attributeType );
                    }
                    else if ( elementName.equals( GraphMLTokens.NODE ) )
                    {
                        String vertexStringId = reader.getAttributeValue( null,
                                GraphMLTokens.ID );

                        Object vertexObjectId = vertexIdMap.get( vertexStringId );
                        if ( vertexObjectId != null )
                            currentVertex = graph.getNodeById( (Long) vertexObjectId );
                        else
                        {
                            currentVertex = graph.createNode();
                            vertexIdMap.put( vertexStringId,
                                    currentVertex.getId() );
                        }

                    }
                    else if ( elementName.equals( GraphMLTokens.EDGE ) )
                    {
                        String edgeLabel = reader.getAttributeValue( null,
                                GraphMLTokens.LABEL );
                        edgeLabel = edgeLabel == null ? GraphMLTokens._DEFAULT
                                : edgeLabel;
                        String outStringId = reader.getAttributeValue( null,
                                GraphMLTokens.SOURCE );
                        String inStringId = reader.getAttributeValue( null,
                                GraphMLTokens.TARGET );

                        // TODO: current edge retrieve by id first?
                        Object outObjectId = vertexIdMap.get( outStringId );
                        Object inObjectId = vertexIdMap.get( inStringId );

                        Node outVertex = null;
                        if ( null != outObjectId )
                            outVertex = graph.getNodeById( (Long) outObjectId );

                        Node inVertex = null;
                        if ( null != inObjectId )
                            inVertex = graph.getNodeById( (Long) inObjectId );

                        if ( null == outVertex )
                        {
                            graph.createNode();
                            outVertex = graph.createNode();
                            vertexIdMap.put( outStringId, outVertex.getId() );
                        }
                        if ( null == inVertex )
                        {
                            inVertex = graph.createNode();
                            vertexIdMap.put( inStringId, inVertex.getId() );
                        }

                        currentEdge = outVertex.createRelationshipTo( inVertex,
                                DynamicRelationshipType.withName( edgeLabel ) );

                    }
                    else if ( elementName.equals( GraphMLTokens.DATA ) )
                    {
                        String key = reader.getAttributeValue( null,
                                GraphMLTokens.KEY );
                        String attributeName = keyIdMap.get( key );
                        if ( attributeName != null )
                        {
                            String value = reader.getElementText();
                            if ( currentVertex != null )
                            {
                                currentVertex.setProperty(
                                        key,
                                        typeCastValue( key, value, keyTypesMaps ) );
                            }
                            else if ( currentEdge != null )
                            {
                                currentEdge.setProperty(
                                        key,
                                        typeCastValue( key, value, keyTypesMaps ) );
                            }
                        }
                    }
                }
                else if ( eventType.equals( XMLEvent.END_ELEMENT ) )
                {
                    String elementName = reader.getName().getLocalPart();
                    if ( elementName.equals( GraphMLTokens.NODE ) )
                        currentVertex = null;
                    else if ( elementName.equals( GraphMLTokens.EDGE ) )
                        currentEdge = null;

                }
            }
        }
        finally
        {
            reader.close();
            tx.success();
            tx.finish();
        }
    }

    public static Object typeCastValue( String key, String value,
            Map<String, String> keyTypes )
    {
        String type = keyTypes.get( key );
        if ( null == type || type.equals( GraphMLTokens.STRING ) )
            return value;
        else if ( type.equals( GraphMLTokens.FLOAT ) )
            return Float.valueOf( value );
        else if ( type.equals( GraphMLTokens.INT ) )
            return Integer.valueOf( value );
        else if ( type.equals( GraphMLTokens.DOUBLE ) )
            return Double.valueOf( value );
        else if ( type.equals( GraphMLTokens.BOOLEAN ) )
            return Boolean.valueOf( value );
        else if ( type.equals( GraphMLTokens.LONG ) )
            return Long.valueOf( value );
        else
            return value;
    }
}

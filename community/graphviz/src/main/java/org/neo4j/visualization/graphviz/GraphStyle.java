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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class GraphStyle
{
	GraphStyle( StyleParameter... parameters )
	{
		this.configuration = new DefaultStyleConfiguration( parameters );
		this.nodeStyle = new DefaultNodeStyle( configuration );
		this.edgeStyle = new DefaultRelationshipStyle( configuration );
	}

	/**
	 * Constructor for subclasses. Used to provide graph styles with more
	 * elaborate configuration than the default configuration parameters can
	 * provide.
	 * @param nodeStyle
	 *            the node style to use.
	 * @param edgeStyle
	 *            the relationship style to use.
	 */
    public GraphStyle( NodeStyle nodeStyle, RelationshipStyle edgeStyle )
	{
        this.configuration = null;
		this.nodeStyle = nodeStyle;
		this.edgeStyle = edgeStyle;
	}

	/**
	 * Emit the end of a graph. Override this method to change the format of the
	 * end of a graph.
	 * @param stream
	 *            the stream to emit the graph ending to.
	 * @throws IOException
	 *             if there is an error in emitting the graph ending.
	 */
	protected void emitGraphEnd( Appendable stream ) throws IOException
	{
		stream.append( "}\n" );
	}

	/**
	 * Emit the start of a graph. Override this method to change the format of
	 * the start of a graph.
	 * @param stream
	 *            the stream to emit the graph start to.
	 * @throws IOException
	 *             if there is an error in emitting the graph start.
	 */
	protected void emitGraphStart( Appendable stream ) throws IOException
	{
		stream.append( "digraph Neo {\n" );
		emitHeaders(stream);
	}

	protected void emitHeaders( Appendable stream ) throws IOException
    {
        if ( configuration != null )
        {
            configuration.emitHeader( stream );
        }
        stream.append( "  node [\n" );
        if ( configuration != null )
        {
            configuration.emitHeaderNode( stream );
        }
        else
        {
            header().emitNode( stream );
        }
        stream.append( "  ]\n" );
        stream.append( "  edge [\n" );
        if ( configuration != null )
        {
            configuration.emitHeaderEdge( stream );
        }
        else
        {
            header().emitEdge( stream );
        }
        stream.append( "  ]\n" );

        
    }

    final NodeStyle nodeStyle;
	final RelationshipStyle edgeStyle;
	private final DefaultStyleConfiguration configuration;
	private static volatile Header header;

    GraphStyle getSubgraphStyle( final String subgraphName )
    {
        return new GraphStyle( nodeStyle, edgeStyle )
        {
            @Override
            protected void emitGraphStart( Appendable stream ) throws IOException
            {
                stream.append( String.format( " subgraph cluster_%s {", subgraphName ) );
            }

            @Override
            protected void emitGraphEnd( Appendable stream ) throws IOException
            {
                stream.append( String.format( " label = \"%s\"\n }\n", subgraphName ) );
            }
        };
    }

	static Header header()
	{
		Header instance = header;
		if ( instance == null )
		{
			synchronized ( GraphStyle.class )
			{
				instance = header;
				if ( instance == null )
				{
					header = instance = new Header();
				}
			}
		}
		return instance;
	}

	static final class Header
	{
		private Header()
		{
			String prefix = getClass().getPackage().getName() + ".";
			String nodePrefix = prefix + "node.";
			String relPrefix = prefix + "relationship.";
			Properties properties = System.getProperties();
			for ( Object obj : properties.keySet() )
			{
				try
				{
					String property = ( String ) obj;
					if ( property.startsWith( nodePrefix ) )
					{
						nodeHeader.put( property
						    .substring( nodePrefix.length() ),
						    ( String ) properties.get( property ) );
					}
					else if ( property.startsWith( relPrefix ) )
					{
						edgeHeader.put(
						    property.substring( relPrefix.length() ),
						    ( String ) properties.get( property ) );
					}
				}
				catch ( ClassCastException cce )
				{
					continue;
				}
			}
			assertMember( nodeHeader, properties, prefix, "fontname",
			    "Bitstream Vera Sans" );
			assertMember( nodeHeader, properties, prefix, "fontsize", "8" );
			assertMember( edgeHeader, properties, prefix, "fontname",
			    "Bitstream Vera Sans" );
			assertMember( edgeHeader, properties, prefix, "fontsize", "8" );
			nodeHeader.put( "shape", "Mrecord" );
		}

		final Map<String, String> nodeHeader = new HashMap<String, String>();
		final Map<String, String> edgeHeader = new HashMap<String, String>();
        final Map<String, String> graphHeader = new HashMap<String, String>();

		private void assertMember( Map<String, String> header,
		    Properties properties, String prefix, String key, String def )
		{
			if ( !header.containsKey( key ) )
			{
				header.put( key, properties.getProperty( prefix + key, def ) );
			}
		}

		private void emitNode( Appendable stream ) throws IOException
		{
			emitHeader( stream, nodeHeader );
		}

		private void emitEdge( Appendable stream ) throws IOException
		{
			emitHeader( stream, edgeHeader );
		}
	}

	static void emitHeader( Appendable stream, Map<String, String> header )
	    throws IOException
	{
		for ( String key : header.keySet() )
		{
			stream.append( "    " + key + " = \"" + header.get( key ) + "\"\n" );
		}
	}
	

}

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

import org.neo4j.function.Predicate;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.visualization.PropertyType;

class DefaultStyleConfiguration implements StyleConfiguration
{
	boolean displayRelationshipLabel = true;

    DefaultStyleConfiguration( StyleParameter... parameters )
	{
		this.nodeHeader = new HashMap<String, String>(
		    GraphStyle.header().nodeHeader );
		this.edgeHeader = new HashMap<String, String>(
		    GraphStyle.header().edgeHeader );
		this.header = new HashMap<String, String>(
		    GraphStyle.header().graphHeader );
		for ( StyleParameter parameter : parameters )
		{
			parameter.configure( this );
		}
	}

    public String escapeLabel( String label )
    {
        label = label.replace( "\\", "\\\\" );
        label = label.replace( "\"", "\\\"" );
        label = label.replace( "'", "\\'" );
        label = label.replace( "\n", "\\n" );
        label = label.replace( "<", "\\<" );
        label = label.replace( ">", "\\>" );
        label = label.replace( "[", "\\[" );
        label = label.replace( "]", "\\]" );
        label = label.replace( "{", "\\{" );
        label = label.replace( "}", "\\}" );
        label = label.replace( "|", "\\|" );

        return label;
    }

	private final Map<String, String> header;
	private final Map<String, String> nodeHeader;
	private final Map<String, String> edgeHeader;
	private final Map<String, ParameterGetter<? super Node>> nodeParams = new HashMap<String, ParameterGetter<? super Node>>();
	private final Map<String, ParameterGetter<? super Relationship>> edgeParams = new HashMap<String, ParameterGetter<? super Relationship>>();
	private PropertyFilter nodeFilter = null;
	private PropertyFilter edgeFilter = null;
	private TitleGetter<? super Node> nodeTitle = null;
	private TitleGetter<? super Relationship> edgeTitle = null;
	private PropertyFormatter nodeFormat = null;
	private PropertyFormatter edgeFormat = null;
    private Predicate<Relationship> reversedRelationshipOrder = null;

    boolean reverseOrder( Relationship edge )
    {
        return reversedRelationshipOrder != null && reversedRelationshipOrder.test( edge );
    }

    void emitHeader( Appendable stream ) throws IOException
    {
        GraphStyle.emitHeader( stream, header );
    }

	void emitHeaderNode( Appendable stream ) throws IOException
	{
		GraphStyle.emitHeader( stream, nodeHeader );
	}

	void emitHeaderEdge( Appendable stream ) throws IOException
	{
		GraphStyle.emitHeader( stream, edgeHeader );
	}

	void emit( Node node, Appendable stream ) throws IOException
	{
		emit( node, nodeParams, stream );
	}

	void emit( Relationship edge, Appendable stream ) throws IOException
	{
		emit( edge, edgeParams, stream );
	}

	private <C extends PropertyContainer> void emit( C container,
	    Map<String, ParameterGetter<? super C>> params, Appendable stream )
	    throws IOException
	{
		for ( String key : params.keySet() )
		{
			String value = params.get( key ).getParameterValue( container, key );
			if ( value != null )
			{
				stream.append( "    " + key + " = \"" + value + "\"\n" );
			}
		}
	}

	String getTitle( Node node )
	{
		if ( nodeTitle != null )
		{
			return nodeTitle.getTitle( node );
		}
		else
		{
			return node.toString();
		}
	}

	String getTitle( Relationship edge )
	{
		if ( edgeTitle != null )
		{
			return edgeTitle.getTitle( edge );
		}
		else
		{
			return edge.getType().name();
		}
	}

	boolean acceptNodeProperty( String key )
	{
		if ( nodeFilter != null )
		{
			return nodeFilter.acceptProperty( key );
		}
		else
		{
			return true;
		}
	}

	boolean acceptEdgeProperty( String key )
	{
		if ( edgeFilter != null )
		{
			return edgeFilter.acceptProperty( key );
		}
		else
		{
			return true;
		}
	}

	void emitNodeProperty( Appendable stream, String key, PropertyType type,
	    Object value ) throws IOException
	{
		if ( nodeFormat != null )
		{
			stream.append( nodeFormat.format( key, type, value ) + "\\l" );
		}
		else
		{
			stream.append( PropertyType.STRING.format(key) + " = " + PropertyType.format( value ) + " : "
			    + type.typeName + "\\l" );
		}
	}

	void emitRelationshipProperty( Appendable stream, String key,
	    PropertyType type, Object value ) throws IOException
	{
		if ( edgeFormat != null )
		{
			stream.append( edgeFormat.format( key, type, value ) + "\\l" );
		}
		else
		{
			stream.append( PropertyType.STRING.format(key) + " = " + PropertyType.format( value ) + " : "
			    + type.typeName + "\\l" );
		}
	}

    public void setRelationshipReverseOrderPredicate( org.neo4j.helpers.Predicate<Relationship> reversed )
    {
		setRelationshipReverseOrderPredicate( org.neo4j.helpers.Predicates.upgrade( reversed ) );
    }

	public void setRelationshipReverseOrderPredicate( Predicate<Relationship> reversed )
	{
		reversedRelationshipOrder = reversed;
	}

    public void setGraphProperty( String property, String value )
	{
        header.put( property, value );
	}

	public void setDefaultNodeProperty( String property, String value )
	{
		nodeHeader.put( property, value );
	}

	public void setDefaultRelationshipProperty( String property, String value )
	{
		edgeHeader.put( property, value );
	}

	public void displayRelationshipLabel( boolean on )
	{
		displayRelationshipLabel = on;
	}

	public void setNodeParameterGetter( String key,
	    ParameterGetter<? super Node> getter )
	{
		nodeParams.put( key, getter );
	}

	public void setNodePropertyFilter( PropertyFilter filter )
	{
		nodeFilter = filter;
	}

	public void setNodeTitleGetter( TitleGetter<? super Node> getter )
	{
		nodeTitle = getter;
	}

	public void setRelationshipParameterGetter( String key,
	    ParameterGetter<? super Relationship> getter )
	{
		edgeParams.put( key, getter );
	}

	public void setRelationshipPropertyFilter( PropertyFilter filter )
	{
		edgeFilter = filter;
	}

	public void setRelationshipTitleGetter(
	    TitleGetter<? super Relationship> getter )
	{
		edgeTitle = getter;
	}

	public void setNodePropertyFomatter( PropertyFormatter format )
	{
		this.nodeFormat = format;
	}

	public void setRelationshipPropertyFomatter( PropertyFormatter format )
	{
		this.edgeFormat = format;
	}
}

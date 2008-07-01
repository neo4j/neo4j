/*
 * Copyright 2008 Network Engine for Objects in Lund AB [neotechnology.com]
 * 
 * This program is free software: you can redistribute it and/or modify
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.visualization.graphviz;

import java.io.IOException;

import org.neo4j.api.core.Node;
import org.neo4j.visualization.PropertyType;

class DefaultNodeStyle implements NodeStyle
{
	private final DefaultStyleConfiguration config;

	DefaultNodeStyle( DefaultStyleConfiguration configuration )
	{
		this.config = configuration;
	}

	public void emitNodeStart( Appendable stream, Node node )
	    throws IOException
	{
		stream.append( "  N" + node.getId() + " [\n" );
		config.emit( node, stream );
		stream.append( "    label = \"{" + config.getTitle( node ) + "|" );
	}

	public void emitEnd( Appendable stream ) throws IOException
	{
		stream.append( "}\"\n  ]\n" );
	}

	public void emitProperty( Appendable stream, String key, Object value )
	    throws IOException
	{
		PropertyType type = PropertyType.getTypeOf( value );
		config.emitRelationshipProperty( stream, key, type, value );
	}
}

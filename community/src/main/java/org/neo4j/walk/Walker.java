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
package org.neo4j.walk;

import java.util.HashSet;
import java.util.Set;

import org.neo4j.api.core.Node;
import org.neo4j.api.core.Relationship;
import org.neo4j.api.core.RelationshipType;

/*
 * This should really be refactored... or really replaced with the new traverser API.
 */
public final class Walker
{
	private Set<Node> nodes;
	private RelationshipType[] types;

	public Walker( Iterable<Node> nodes, RelationshipType... types )
	{
		this.nodes = new HashSet<Node>();
		for ( Node node : nodes )
		{
			this.nodes.add( node );
		}
		this.types = types;
	}

	public <R, E extends Throwable> R accept( Visitor<R, E> visitor ) throws E
	{
		for ( Node node : nodes )
		{
			visitor.visitNode( node );
			for ( Relationship relationship : node.getRelationships( types ) )
			{
				if ( nodes.contains( relationship.getOtherNode( node ) ) )
				{
					visitor.visitRelationship( relationship );
				}
			}
		}
		return visitor.done();
	}
}

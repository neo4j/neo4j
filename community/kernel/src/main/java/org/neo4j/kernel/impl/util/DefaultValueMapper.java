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
package org.neo4j.kernel.impl.util;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.kernel.impl.core.EmbeddedProxySPI;
import org.neo4j.kernel.impl.core.NodeProxy;
import org.neo4j.kernel.impl.core.PathProxy;
import org.neo4j.kernel.impl.core.RelationshipProxy;
import org.neo4j.values.ValueMapper;
import org.neo4j.values.virtual.NodeValue;
import org.neo4j.values.virtual.PathValue;
import org.neo4j.values.virtual.RelationshipValue;
import org.neo4j.values.virtual.VirtualNodeValue;
import org.neo4j.values.virtual.VirtualRelationshipValue;

public class DefaultValueMapper extends ValueMapper.JavaMapper
{
    private final EmbeddedProxySPI proxySPI;

    public DefaultValueMapper( EmbeddedProxySPI proxySPI )
    {
        this.proxySPI = proxySPI;
    }

    @Override
    public Node mapNode( VirtualNodeValue value )
    {
        if ( value instanceof NodeProxyWrappingNodeValue )
        { // this is the back door through which "virtual nodes" slip
            return ((NodeProxyWrappingNodeValue) value).nodeProxy();
        }
        return new NodeProxy( proxySPI, value.id() );
    }

    @Override
    public Relationship mapRelationship( VirtualRelationshipValue value )
    {
        if ( value instanceof RelationshipProxyWrappingValue )
        { // this is the back door through which "virtual relationships" slip
            return ((RelationshipProxyWrappingValue) value).relationshipProxy();
        }
        return new RelationshipProxy( proxySPI, value.id() );
    }

    @Override
    public Path mapPath( PathValue value )
    {
        NodeValue[] nodeValues = value.nodes();
        RelationshipValue[] relationshipValues = value.relationships();
        long[] nodes = new long[nodeValues.length];
        long[] relationships = new long[relationshipValues.length];
        int[] directedTypes = new int[relationshipValues.length];
        for ( int i = 0; i < nodes.length; i++ )
        {
            nodes[i] = nodeValues[i].id();
        }
        for ( int i = 0; i < relationships.length; i++ )
        {
            RelationshipValue relationship = relationshipValues[i];
            relationships[i] = relationship.id();
            int typeId = proxySPI.getRelationshipTypeIdByName( relationship.type().stringValue() );
            directedTypes[i] = nodes[i] == relationship.startNode().id() ? typeId : ~typeId;
        }
        return new PathProxy( proxySPI, nodes, relationships, directedTypes );
    }
}

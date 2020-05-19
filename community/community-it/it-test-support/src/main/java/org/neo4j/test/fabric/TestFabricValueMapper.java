/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.test.fabric;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.kernel.impl.util.DefaultValueMapper;
import org.neo4j.kernel.impl.util.NodeEntityWrappingNodeValue;
import org.neo4j.kernel.impl.util.RelationshipEntityWrappingValue;
import org.neo4j.values.virtual.VirtualNodeValue;
import org.neo4j.values.virtual.VirtualRelationshipValue;

public class TestFabricValueMapper extends DefaultValueMapper
{
    public TestFabricValueMapper()
    {
        super( null );
    }

    @Override
    public Node mapNode( VirtualNodeValue value )
    {
        if ( value instanceof NodeEntityWrappingNodeValue )
        { // this is the back door through which "virtual nodes" slip
            return ((NodeEntityWrappingNodeValue) value).nodeEntity();
        }
        throw new UnsupportedOperationException( "can't map VirtualNodeValue" );
    }

    @Override
    public Relationship mapRelationship( VirtualRelationshipValue value )
    {
        if ( value instanceof RelationshipEntityWrappingValue )
        { // this is the back door through which "virtual relationships" slip
            return ((RelationshipEntityWrappingValue) value).relationshipEntity();
        }
        throw new UnsupportedOperationException( "can't map VirtualRelationshipValue" );
    }
}

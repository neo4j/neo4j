/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.javacompat;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.spatial.Point;
import org.neo4j.kernel.impl.util.BaseToObjectValueWriter;
import org.neo4j.kernel.impl.core.NodeManager;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.Values;

public class ValueToObjectSerializer extends BaseToObjectValueWriter<RuntimeException>
{
    private final NodeManager nodeManager;
    public ValueToObjectSerializer( NodeManager nodeManager )
    {
        super();
        this.nodeManager = nodeManager;
    }

    @Override
    protected Node newNodeProxyById( long id )
    {
        return nodeManager.newNodeProxyById( id );
    }

    @Override
    protected Relationship newRelationshipProxyById( long id )
    {
        return nodeManager.newRelationshipProxyById( id );
    }

    @Override
    protected Point newPoint( CoordinateReferenceSystem crs, double[] coordinate )
    {
        return Values.pointValue( crs, coordinate );
    }
}

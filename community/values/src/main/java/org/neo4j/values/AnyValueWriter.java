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
package org.neo4j.values;

import org.neo4j.values.storable.TextArray;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.ValueWriter;
import org.neo4j.values.virtual.CoordinateReferenceSystem;
import org.neo4j.values.virtual.EdgeValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.NodeValue;

/**
 * Writer of any values.
 */
public interface AnyValueWriter<E extends Exception> extends ValueWriter<E>
{
    void writeNodeReference( long nodeId ) throws E;

    void writeNode( long nodeId, TextArray labels, MapValue properties ) throws E;

    void writeEdgeReference( long edgeId ) throws E;

    void writeEdge( long edgeId, long startNodeId, long endNodeId, TextValue type, MapValue properties ) throws E;

    void beginMap( int size ) throws E;

    void endMap() throws E;

    void beginList( int size ) throws E;

    void endList() throws E;

    void writePath( NodeValue[] nodes, EdgeValue[] edges ) throws E;

    void beginPoint( CoordinateReferenceSystem coordinateReferenceSystem ) throws E;

    void endPoint() throws E;

    default void writeVirtualNodeHack( Object node )
    {
        // do nothing, this is an ugly hack.
    }

    default void writeVirtualEdgeHack( Object relationship )
    {
        // do nothing, this is an ugly hack.
    }
}

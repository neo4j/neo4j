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

/**
 * Writer of any values.
 */
public interface AnyValueWriter extends ValueWriter
{

    void beginNode( long nodeId );

    void endNode();

    void beginLabels( int numberOfLabels );

    void writeLabel( int labelId );

    void endLabels();

    void beginProperties( int numberOfProperties );

    void writePropertyKeyId( int propertyKeyId );

    void endProperties();

    void beginEdge( long edgeId );

    void endEdge();

    void beginMap( int size );

    void writeKeyId( int keyId );

    void endMap();

    void beginList( int size );

    void endList();

    void beginPath( int length );

    void endPath();
}

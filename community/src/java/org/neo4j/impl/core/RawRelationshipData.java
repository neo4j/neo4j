/*
 * Copyright (c) 2002-2008 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.impl.core;

public class RawRelationshipData
{
    private final int id;
    private final int firstNode;
    private final int secondNode;
    private final int type;

    public RawRelationshipData( int id, int firstNode, int secondNode, int type )
    {
        this.id = id;
        this.firstNode = firstNode;
        this.secondNode = secondNode;
        this.type = type;
    }

    int getId()
    {
        return this.id;
    }

    public int getFirstNode()
    {
        return firstNode;
    }

    public int getSecondNode()
    {
        return secondNode;
    }

    public int getType()
    {
        return type;
    }
}
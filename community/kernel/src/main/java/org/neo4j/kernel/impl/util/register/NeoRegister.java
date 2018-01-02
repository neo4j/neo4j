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
package org.neo4j.kernel.impl.util.register;

/** Domain-specific registers */
public interface NeoRegister
{
    interface RelationshipRegister extends Relationship.In, Relationship.Out { }
    interface Relationship
    {
        static final long NO_REL = -1;
        interface In
        {
            long read();
        }

        interface Out
        {
            void write(long relId);
        }
    }

    interface NodeRegister extends Node.In, Node.Out { }
    interface Node
    {
        static final long NO_NODE = -1;
        interface In
        {
            long read();
        }

        interface Out
        {
            void write(long nodeId);
        }
    }

    interface RelTypeRegister extends RelType.In, RelType.Out { }
    interface RelType
    {
        static final int NO_TYPE = -1;
        interface In
        {
            int read();
        }

        interface Out
        {
            void write(int relId);
        }
    }

}

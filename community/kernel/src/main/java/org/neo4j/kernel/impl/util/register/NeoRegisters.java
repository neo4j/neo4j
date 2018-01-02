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

public class NeoRegisters
{
    public static NeoRegister.NodeRegister newNodeRegister()
    {
        return newNodeRegister( NeoRegister.Node.NO_NODE );
    }

    public static NeoRegister.NodeRegister newNodeRegister( final long initialValue )
    {
        return new NeoRegister.NodeRegister()
        {
            private long id = initialValue;

            @Override
            public long read()
            {
                return id;
            }

            @Override
            public void write( long nodeId )
            {
                this.id = nodeId;
            }
        };
    }

    public static NeoRegister.RelationshipRegister newRelationshipRegister()
    {
        return newRelationshipRegister( NeoRegister.Relationship.NO_REL );
    }

    public static NeoRegister.RelationshipRegister newRelationshipRegister(final long initialValue)
    {
        return new NeoRegister.RelationshipRegister()
        {
            private long id = initialValue;

            @Override
            public long read()
            {
                return id;
            }

            @Override
            public void write( long relId )
            {
                this.id = relId;
            }
        };
    }

    public static NeoRegister.RelTypeRegister newRelTypeRegister()
    {
        return newRelTypeRegister( NeoRegister.RelType.NO_TYPE );
    }

    public static NeoRegister.RelTypeRegister newRelTypeRegister(final int initialValue)
    {
        return new NeoRegister.RelTypeRegister()
        {
            private int id = initialValue;

            @Override
            public int read()
            {
                return id;
            }

            @Override
            public void write( int type )
            {
                this.id = type;
            }
        };
    }
}

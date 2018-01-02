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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.neo4j.cursor.Cursor;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.impl.util.register.NeoRegister;
import org.neo4j.register.Register;

public class ExpandTestUtils
{
    public static List<Row> rows( Cursor cursor, NeoRegister.RelationshipRegister relId, NeoRegister.RelTypeRegister
            relType, Register.ObjectRegister<Direction> direction, NeoRegister.NodeRegister startId,
                                  NeoRegister.NodeRegister neighborId )
    {
        List<Row> result = new ArrayList<>();
        while(cursor.next())
        {
            result.add( row( relId.read(), relType.read(), direction.read(), startId.read(), neighborId.read() ) );
        }
        Collections.sort( result, new Comparator<Row>()
        {
            @Override
            public int compare( Row o1, Row o2 )
            {
                if ( o1.relId == o2.relId )
                {
                    return (int) (o1.neighborId - o2.neighborId);
                }
                else
                {
                    return (int) (o1.relId - o2.relId);
                }
            }
        } );
        return result;
    }

    public static Row row( long relId, int relType, Direction direction, long startId, long neighborId )
    {
        return new Row( relId, relType, direction, startId, neighborId );
    }

    public static class Row
    {
        public final long relId;
        public final int type;
        public final Direction direction;
        public final long startId;
        public final long neighborId;

        public Row( long relId, int type, Direction direction, long startId, long neighborId )
        {
            this.relId = relId;
            this.type = type;
            this.direction = direction;
            this.startId = startId;

            this.neighborId = neighborId;
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }

            Row row = (Row) o;

            if ( neighborId != row.neighborId )
            {
                return false;
            }
            if ( relId != row.relId )
            {
                return false;
            }
            if ( type != row.type )
            {
                return false;
            }
            if ( startId != row.startId )
            {
                return false;
            }
            if ( direction != row.direction )
            {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode()
        {
            int result = (int) (relId ^ (relId >>> 32));
            result = 31 * result + type;
            result = 31 * result + (direction != null ? direction.hashCode() : 0);
            result = 31 * result + (int) (startId ^ (startId >>> 32));
            result = 31 * result + (int) (neighborId ^ (neighborId >>> 32));
            return result;
        }

        @Override
        public String toString()
        {
            return "row( " + relId + ", " + type + ", " + direction + ", " + startId + ", " + neighborId + ')';
        }
    }

}

/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.core;

import java.util.ArrayList;
import java.util.Collection;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import static org.junit.Assert.assertEquals;

@RunWith( Parameterized.class )
public class RelationshipImplTest
{
    @Parameters
    public static Collection<Object[]> data()
    {
        Collection<Object[]> data = new ArrayList<Object[]>();
        for ( int i = 1; i <= 16; i++ )
        {
            data.add( new Object[] { (1 << i) - 1 } );
        }
        return data;
    }

    private final int typeId;

    public RelationshipImplTest( int typeId )
    {
        this.typeId = typeId;
    }

    @Test
    public void typeIdCanUse16Bits()
    {
        RelationshipImpl rel = new RelationshipImpl( 10, 10, 10, typeId );
        assertEquals( typeId, rel.getTypeId() );
    }
}

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
package org.neo4j.kernel;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith( Parameterized.class )
public class CommunityIdTypeConfigurationProviderTest
{

    private IdType reusableType;

    @Parameterized.Parameters
    public static List<Object[]> data()
    {
        return Arrays.asList( new Object[]{IdType.PROPERTY},
                new Object[]{IdType.STRING_BLOCK},
                new Object[]{IdType.ARRAY_BLOCK},
                new Object[]{IdType.NODE_LABELS} );
    }

    public CommunityIdTypeConfigurationProviderTest( IdType reusableType )
    {
        this.reusableType = reusableType;
    }

    @Test
    public void nonReusableTypeConfiguration()
    {
        IdTypeConfigurationProvider provider = createIdTypeProvider();
        IdTypeConfiguration typeConfiguration = provider.getIdTypeConfiguration( IdType.RELATIONSHIP );
        assertFalse( "Relationship ids are not reusable.", typeConfiguration.allowAggressiveReuse() );
        assertEquals( "Relationship ids are not reusable.", IdTypeConfiguration.DEFAULT_GRAB_SIZE, typeConfiguration.getGrabSize() );
    }

    @Test
    public void reusableTypeConfiguration()
    {
        IdTypeConfigurationProvider provider = createIdTypeProvider();
        IdTypeConfiguration typeConfiguration = provider.getIdTypeConfiguration( reusableType );
        assertTrue( typeConfiguration.allowAggressiveReuse() );
        assertEquals( IdTypeConfiguration.AGGRESIVE_GRAB_SIZE, typeConfiguration.getGrabSize() );
    }

    private IdTypeConfigurationProvider createIdTypeProvider()
    {
        return new CommunityIdTypeConfigurationProvider();
    }

}
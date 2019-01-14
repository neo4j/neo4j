/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.impl.api.store;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;

import org.neo4j.internal.kernel.api.IndexCapability;
import org.neo4j.internal.kernel.api.IndexReference;
import org.neo4j.kernel.api.index.IndexProvider;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.neo4j.kernel.api.schema.SchemaTestUtil.simpleNameLookup;

@RunWith( Parameterized.class )
public class IndexReferenceTest
{
    private final String expectedUserDescription;
    private final IndexReference indexReference;

    @Parameterized.Parameters( name = "{0}" )
    public static Iterable<Object[]> data()
    {
        return Arrays.asList(
                new Object[]{"Index( GENERAL, :Label1(property2) )", DefaultIndexReference.general( 1, 2 )},
                new Object[]{"Index( UNIQUE, :Label10(property200) )", DefaultIndexReference.unique( 10, 200 )},
                new Object[]{"Index( GENERAL, :Label42(property3) )", defaultCapableIndexReference( false, 42, 3 )},
                new Object[]{"Index( UNIQUE, :Label42(property3) )", defaultCapableIndexReference( true, 42, 3 )},
                new Object[]{"Index( GENERAL, :Label1(property2, property20) )", DefaultIndexReference.general( 1, 2, 20 )},
                new Object[]{"Index( UNIQUE, :Label10(property200, property2000) )", DefaultIndexReference.unique( 10, 200, 2000 )},
                new Object[]{"Index( GENERAL, :Label42(property3, property30) )", defaultCapableIndexReference( false, 42, 3, 30 )},
                new Object[]{"Index( UNIQUE, :Label42(property3, property30) )", defaultCapableIndexReference( true, 42, 3, 30 )}
        );
    }

    public IndexReferenceTest( String expectedUserDescription, IndexReference indexReference )
    {
        this.expectedUserDescription = expectedUserDescription;
        this.indexReference = indexReference;
    }

    @Test
    public void shouldGiveNiceUserDescriptions()
    {
        assertThat( indexReference.userDescription( simpleNameLookup ), equalTo( expectedUserDescription ) );
    }

    private static DefaultCapableIndexReference defaultCapableIndexReference( boolean unique, int labelId, int... propertyIds )
    {
        return new DefaultCapableIndexReference( unique, IndexCapability.NO_CAPABILITY,
                new IndexProvider.Descriptor( "no-desc", "1.0" ), labelId, propertyIds );
    }
}

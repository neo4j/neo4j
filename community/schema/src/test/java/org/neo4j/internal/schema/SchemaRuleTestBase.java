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
package org.neo4j.internal.schema;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

abstract class SchemaRuleTestBase
{
    static final long RULE_ID = 1;
    static final long RULE_ID_2 = 2;
    static final int LABEL_ID = 10;
    static final int REL_TYPE_ID = 20;
    static final int PROPERTY_ID_1 = 30;
    static final int PROPERTY_ID_2 = 31;

    private static final String PROVIDER_KEY = "index-provider";
    private static final String PROVIDER_VERSION = "1.0";
    static final IndexProviderDescriptor PROVIDER = new IndexProviderDescriptor( PROVIDER_KEY, PROVIDER_VERSION );

    void assertEquality( Object o1, Object o2 )
    {
        assertThat( o1, equalTo( o2 ) );
        assertThat( o2, equalTo( o1 ) );
        assertThat( o1.hashCode(), equalTo( o2.hashCode() ) );
    }

    public static IndexPrototype forLabel( int labelId, int... propertyIds )
    {
        return IndexPrototype.forSchema( SchemaDescriptor.forLabel( labelId, propertyIds ), PROVIDER );
    }

    public static IndexPrototype namedForLabel( String name, int labelId, int... propertyIds )
    {
        return IndexPrototype.forSchema( SchemaDescriptor.forLabel( labelId, propertyIds ), PROVIDER ).withName( name );
    }

    public static IndexPrototype uniqueForLabel( int labelId, int... propertyIds )
    {
        return IndexPrototype.uniqueForSchema( SchemaDescriptor.forLabel( labelId, propertyIds ), PROVIDER );
    }

    public static IndexPrototype namedUniqueForLabel( String name, int labelId, int... propertyIds )
    {
        return IndexPrototype.uniqueForSchema( SchemaDescriptor.forLabel( labelId, propertyIds ), PROVIDER ).withName( name );
    }
}

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
package org.neo4j.kernel.impl.store.record;

import org.neo4j.kernel.api.index.IndexProvider;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

abstract class SchemaRuleTestBase
{
    protected static final long RULE_ID = 1;
    protected static final long RULE_ID_2 = 2;
    protected static final int LABEL_ID = 10;
    protected static final int REL_TYPE_ID = 20;
    protected static final int PROPERTY_ID_1 = 30;
    protected static final int PROPERTY_ID_2 = 31;

    protected static final IndexProvider.Descriptor PROVIDER_DESCRIPTOR =
            new IndexProvider.Descriptor( "index-provider", "1.0" );
    protected static final IndexProvider.Descriptor PROVIDER_DESCRIPTOR_2 =
            new IndexProvider.Descriptor( "index-provider-2", "2.0" );

    protected void assertEquality( Object o1, Object o2 )
    {
        assertThat( o1, equalTo( o2 ) );
        assertThat( o2, equalTo( o1 ) );
        assertThat( o1.hashCode(), equalTo( o2.hashCode() ) );
    }
}

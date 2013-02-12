/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.api;

import static org.junit.Assert.assertEquals;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.kernel.impl.nioneo.store.IndexRule.State.POPULATING;

import java.util.Arrays;
import java.util.Collection;

import org.junit.Test;
import org.neo4j.kernel.impl.nioneo.store.IndexRule;
import org.neo4j.kernel.impl.nioneo.store.SchemaRule;

public class SchemaCacheTest
{
    final IndexRule hans = createIndexRule( 1, 0, 5 );
    final IndexRule witch = createIndexRule( 2, 3, 6 );
    final IndexRule gretel = createIndexRule( 3, 0, 7 );

    @Test
    public void should_construct_schema_cache()
    {
        // GIVEN
        Collection<SchemaRule> rules = Arrays.<SchemaRule>asList( hans, witch, gretel );
        SchemaCache cache = new SchemaCache( rules );

        // THEN
        assertEquals( asSet( hans, gretel ), asSet( cache.getSchemaRules( 0 ) ) );
        assertEquals( asSet( witch ), asSet( cache.getSchemaRules( 3 ) ) );
        assertEquals( asSet( rules ), asSet( cache.getSchemaRules() ) );
    }

    @Test
    public void should_add_schema_rules_to_a_label() {
        // GIVEN
        Collection<SchemaRule> rules = Arrays.<SchemaRule>asList( );
        SchemaCache cache = new SchemaCache( rules );

        // WHEN
        cache.addSchemaRule( hans );
        cache.addSchemaRule( gretel );

        // THEN
        assertEquals( asSet( hans, gretel ), asSet( cache.getSchemaRules( 0 ) ) );
    }

    @Test
    public void should_to_retrieve_all_schema_rules()
    {
        // GIVEN
        Collection<SchemaRule> rules = Arrays.<SchemaRule>asList( );
        SchemaCache cache = new SchemaCache( rules );

        // WHEN
        cache.addSchemaRule( hans );
        cache.addSchemaRule( gretel );

        // THEN
        assertEquals( asSet( hans, gretel ), asSet( cache.getSchemaRules( ) ) );
    }

    private IndexRule createIndexRule( long id, long label, long propertyKey )
    {
        return new IndexRule( id, label, POPULATING, new long[] {propertyKey} );
    }
}

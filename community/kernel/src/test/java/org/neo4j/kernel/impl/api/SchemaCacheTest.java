package org.neo4j.kernel.impl.api;

import static junit.framework.Assert.assertEquals;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;

import java.util.Arrays;
import java.util.Collection;

import org.junit.Test;
import org.neo4j.kernel.impl.nioneo.store.IndexRule;
import org.neo4j.kernel.impl.nioneo.store.SchemaRule;

public class SchemaCacheTest
{
    final IndexRule hans = createIndexRule( 1, 0, "hans" );
    final IndexRule witch = createIndexRule( 2, 3, "witch" );
    final IndexRule gretel = createIndexRule( 3, 0, "gretel" );

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

    private IndexRule createIndexRule( long id, long label, String propertyKey )
    {
        return new IndexRule( id, label, propertyKey );
    }
}

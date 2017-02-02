/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.impl.store.record;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.stream.IntStream;

import org.neo4j.kernel.api.exceptions.schema.MalformedSchemaRuleException;
import org.neo4j.kernel.api.schema_new.index.NewIndexDescriptorFactory;
import org.neo4j.storageengine.api.schema.SchemaRule;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.fail;

public class SchemaRuleSerializationTest extends SchemaRuleTestBase
{
    private static final int VERY_LARGE_CAPACITY = 1024;

    @Test
    public void shouldSerializeAndDeserializeIndexRules() throws MalformedSchemaRuleException
    {
        IndexRule regular = IndexRule.indexRule( RULE_ID, NewIndexDescriptorFactory.forLabel( LABEL_ID, PROPERTY_ID_1 ),
                PROVIDER_DESCRIPTOR );
        assertSerializeAndDeserializeIndexRule( RULE_ID, regular );

        IndexRule unique = IndexRule
                .constraintIndexRule( RULE_ID_2, NewIndexDescriptorFactory.uniqueForLabel( LABEL_ID, PROPERTY_ID_1 ),
                        PROVIDER_DESCRIPTOR, RULE_ID );
        assertSerializeAndDeserializeIndexRule( RULE_ID_2, unique );
    }

    @Test
    public void shouldSerializeAndDeserializeCompositeIndexRules() throws MalformedSchemaRuleException
    {
        IndexRule composite = IndexRule.indexRule( RULE_ID,
                NewIndexDescriptorFactory.forLabel( LABEL_ID, PROPERTY_ID_1, PROPERTY_ID_2 ), PROVIDER_DESCRIPTOR );
        assertSerializeAndDeserializeIndexRule( RULE_ID, composite );

        IndexRule compositeUnique = IndexRule.constraintIndexRule( RULE_ID_2,
                NewIndexDescriptorFactory.uniqueForLabel( LABEL_ID, PROPERTY_ID_1, PROPERTY_ID_2 ),
                PROVIDER_DESCRIPTOR, RULE_ID );
        assertSerializeAndDeserializeIndexRule( RULE_ID_2, compositeUnique );
    }

    @Test
    public void shouldSerializeAndDeserialize_Big_CompositeIndexRules() throws MalformedSchemaRuleException
    {
        int[] propertIds = IntStream.range(1, 200).toArray();
        IndexRule composite = IndexRule.indexRule( RULE_ID,
                NewIndexDescriptorFactory.forLabel( LABEL_ID, propertIds ), PROVIDER_DESCRIPTOR );
        assertSerializeAndDeserializeIndexRule( RULE_ID, composite );
    }

    private void assertSerializeAndDeserializeIndexRule( long ruleId, IndexRule indexRule )
            throws MalformedSchemaRuleException
    {
        ByteBuffer buffer = ByteBuffer.allocate( VERY_LARGE_CAPACITY );
        SchemaRuleSerialization.serialize( indexRule, buffer );
        buffer.flip();
        IndexRule deserialized = assertIndexRule( SchemaRuleSerialization.deserialize( ruleId, buffer ) );

        // THEN
        assertThat( deserialized.getId(), equalTo( indexRule.getId() ) );
        assertThat( deserialized.getIndexDescriptor(), equalTo( indexRule.getIndexDescriptor() ) );
        assertThat( deserialized.getSchemaDescriptor(), equalTo( indexRule.getSchemaDescriptor() ) );
        assertThat( deserialized.getProviderDescriptor(), equalTo( indexRule.getProviderDescriptor() ) );
    }

    private IndexRule assertIndexRule( SchemaRule schemaRule )
    {
        if ( !(schemaRule instanceof IndexRule) )
        {
            fail( "Expected IndexRule, but got "+schemaRule.getClass().getSimpleName() );
        }
        return (IndexRule)schemaRule;
    }

}

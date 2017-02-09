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
import java.util.Base64;
import java.util.stream.IntStream;

import org.neo4j.kernel.api.exceptions.schema.MalformedSchemaRuleException;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.api.schema_new.constaints.ConstraintDescriptor;
import org.neo4j.kernel.api.schema_new.constaints.ConstraintDescriptorFactory;
import org.neo4j.kernel.api.schema_new.index.NewIndexDescriptor;
import org.neo4j.kernel.api.schema_new.index.NewIndexDescriptorFactory;
import org.neo4j.storageengine.api.schema.SchemaRule;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.fail;
import static org.neo4j.test.assertion.Assert.assertException;

public class SchemaRuleSerializationTest extends SchemaRuleTestBase
{
    private static final int VERY_LARGE_CAPACITY = 1024;

    IndexRule indexRegular = IndexRule.indexRule( RULE_ID,
            NewIndexDescriptorFactory.forLabel( LABEL_ID, PROPERTY_ID_1 ), PROVIDER_DESCRIPTOR );

    IndexRule indexUnique = IndexRule.constraintIndexRule( RULE_ID_2,
            NewIndexDescriptorFactory.uniqueForLabel( LABEL_ID, PROPERTY_ID_1 ), PROVIDER_DESCRIPTOR, RULE_ID );

    IndexRule indexCompositeRegular = IndexRule.indexRule( RULE_ID,
            NewIndexDescriptorFactory.forLabel( LABEL_ID, PROPERTY_ID_1, PROPERTY_ID_2 ), PROVIDER_DESCRIPTOR );

    IndexRule indexCompositeUnique = IndexRule.constraintIndexRule( RULE_ID_2,
            NewIndexDescriptorFactory.uniqueForLabel( LABEL_ID, PROPERTY_ID_1, PROPERTY_ID_2 ),
            PROVIDER_DESCRIPTOR, RULE_ID );

    IndexRule indexBigComposite = IndexRule.indexRule( RULE_ID,
            NewIndexDescriptorFactory.forLabel( LABEL_ID, IntStream.range(1, 200).toArray() ), PROVIDER_DESCRIPTOR );

    ConstraintRule constraintExistsLabel = ConstraintRule.constraintRule( RULE_ID,
            ConstraintDescriptorFactory.existsForLabel( LABEL_ID, PROPERTY_ID_1 ) );

    ConstraintRule constraintUniqueLabel = ConstraintRule.constraintRule( RULE_ID_2,
            ConstraintDescriptorFactory.uniqueForLabel( LABEL_ID, PROPERTY_ID_1 ), RULE_ID );

    ConstraintRule constraintExistsRelType = ConstraintRule.constraintRule( RULE_ID_2,
            ConstraintDescriptorFactory.existsForRelType( REL_TYPE_ID, PROPERTY_ID_1 ) );

    ConstraintRule constraintUniqueRelType = ConstraintRule.constraintRule( RULE_ID,
            ConstraintDescriptorFactory.uniqueForRelType( REL_TYPE_ID, PROPERTY_ID_1 ), RULE_ID_2 );

    ConstraintRule constraintCompositeLabel = ConstraintRule.constraintRule( RULE_ID,
            ConstraintDescriptorFactory.existsForLabel( LABEL_ID, PROPERTY_ID_1, PROPERTY_ID_2 ) );

    ConstraintRule constraintCompositeRelType = ConstraintRule.constraintRule( RULE_ID_2,
            ConstraintDescriptorFactory.existsForRelType( REL_TYPE_ID, PROPERTY_ID_1, PROPERTY_ID_2 ) );

    // INDEX RULES

    @Test
    public void shouldSerializeAndDeserializeIndexRules() throws MalformedSchemaRuleException
    {
        assertSerializeAndDeserializeIndexRule( indexRegular );
        assertSerializeAndDeserializeIndexRule( indexUnique );
    }

    @Test
    public void shouldSerializeAndDeserializeCompositeIndexRules() throws MalformedSchemaRuleException
    {
        assertSerializeAndDeserializeIndexRule( indexCompositeRegular );
        assertSerializeAndDeserializeIndexRule( indexCompositeUnique );
    }

    @Test
    public void shouldSerializeAndDeserialize_Big_CompositeIndexRules() throws MalformedSchemaRuleException
    {
        assertSerializeAndDeserializeIndexRule( indexBigComposite );
    }

    // CONSTRAINT RULES

    @Test
    public void shouldSerializeAndDeserializeConstraintRules() throws MalformedSchemaRuleException
    {
        assertSerializeAndDeserializeConstraintRule( constraintExistsLabel );
        assertSerializeAndDeserializeConstraintRule( constraintUniqueLabel );
        assertSerializeAndDeserializeConstraintRule( constraintExistsRelType );
        assertSerializeAndDeserializeConstraintRule( constraintUniqueRelType );
    }

    @Test
    public void shouldSerializeAndDeserializeCompositeConstraintRules() throws MalformedSchemaRuleException
    {
        assertSerializeAndDeserializeConstraintRule( constraintCompositeLabel );
        assertSerializeAndDeserializeConstraintRule( constraintCompositeRelType );
    }

    @Test
    public void shouldReturnCorrectLengthForIndexRules() throws MalformedSchemaRuleException
    {
        assertCorrectLength( indexRegular );
        assertCorrectLength( indexUnique );
        assertCorrectLength( indexCompositeRegular );
        assertCorrectLength( indexCompositeUnique );
        assertCorrectLength( indexBigComposite );
    }

    @Test
    public void shouldReturnCorrectLengthForConstraintRules() throws MalformedSchemaRuleException
    {
        assertCorrectLength( constraintExistsLabel );
    }

    // BACKWARDS COMPATIBILITY

    @Test
    public void shouldParseIndexRule() throws Exception
    {
        assertParseIndexRule( "/////wsAAAAOaW5kZXgtcHJvdmlkZXIAAAAEMjUuMB9bAAACAAABAAAABA==" );
        assertParseIndexRule( "AAACAAEAAAAOaW5kZXgtcHJvdmlkZXIAAAAEMjUuMAABAAAAAAAAAAQ=" ); // LEGACY
    }

    @Test
    public void shouldParseUniqueIndexRule() throws Exception
    {
        assertParseUniqueIndexRule( "/////wsAAAAOaW5kZXgtcHJvdmlkZXIAAAAEMjUuMCAAAAAAAAAAC1sAAAA9AAEAAAPc" );
        assertParseUniqueIndexRule( "AAAAPQIAAAAOaW5kZXgtcHJvdmlkZXIAAAAEMjUuMAABAAAAAAAAA9wAAAAAAAAACw==" ); // LEGACY
    }

    @Test
    public void shouldParseUniqueConstraintRule() throws Exception
    {
        assertParseUniqueConstraintRule( "/////ww+AAAAAAAAAAJbAAAANwABAAAAAw==" );
        assertParseUniqueConstraintRule( "AAAANwMBAAAAAAAAAAMAAAAAAAAAAg==" ); // LEGACY
    }

    @Test
    public void shouldParseNodePropertyExistsRule() throws Exception
    {
        assertParseNodePropertyExistsRule( "/////ww9WwAAAC0AAQAAADM=" );
        assertParseNodePropertyExistsRule( "AAAALQQAAAAz" ); // LEGACY
    }

    @Test
    public void shouldParseRelationshipPropertyExistsRule() throws Exception
    {
        assertParseRelationshipPropertyExistsRule( "/////ww9XAAAIUAAAQAAF+c=" ); // LEGACY6
        assertParseRelationshipPropertyExistsRule( "AAAhQAUAABfn" ); // LEGACY6
    }

    private void assertParseIndexRule( String serialized ) throws Exception
    {
        // GIVEN
        long ruleId = 24;
        NewIndexDescriptor index = NewIndexDescriptorFactory.forLabel( 512, 4 );
        SchemaIndexProvider.Descriptor indexProvider = new SchemaIndexProvider.Descriptor( "index-provider", "25.0" );
        byte[] bytes = Base64.getDecoder().decode( serialized );

        // WHEN
        IndexRule deserialized = assertIndexRule( SchemaRuleSerialization.deserialize( ruleId, ByteBuffer.wrap( bytes ) ) );

        // THEN
        assertThat( deserialized.getId(), equalTo( ruleId ) );
        assertThat( deserialized.getIndexDescriptor(), equalTo( index ) );
        assertThat( deserialized.getSchemaDescriptor(), equalTo( index.schema() ) );
        assertThat( deserialized.getProviderDescriptor(), equalTo( indexProvider ) );
        assertException( deserialized::getOwningConstraint, IllegalStateException.class, "" );
    }

    private void assertParseUniqueIndexRule( String serialized ) throws MalformedSchemaRuleException
    {
        // GIVEN
        long ruleId = 33;
        long constraintId = 11;
        NewIndexDescriptor index = NewIndexDescriptorFactory.uniqueForLabel( 61, 988 );
        SchemaIndexProvider.Descriptor indexProvider = new SchemaIndexProvider.Descriptor( "index-provider", "25.0" );
        byte[] bytes = Base64.getDecoder().decode( serialized );

        // WHEN
        IndexRule deserialized = assertIndexRule( SchemaRuleSerialization.deserialize( ruleId, ByteBuffer.wrap( bytes ) ) );

        // THEN
        assertThat( deserialized.getId(), equalTo( ruleId ) );
        assertThat( deserialized.getIndexDescriptor(), equalTo( index ) );
        assertThat( deserialized.getSchemaDescriptor(), equalTo( index.schema() ) );
        assertThat( deserialized.getProviderDescriptor(), equalTo( indexProvider ) );
        assertThat( deserialized.getOwningConstraint(), equalTo( constraintId ) );
    }

    private void assertParseUniqueConstraintRule( String serialized ) throws MalformedSchemaRuleException
    {
        // GIVEN
        long ruleId = 1;
        int propertyKey = 3;
        int labelId = 55;
        long ownedIndexId = 2;
        ConstraintDescriptor constraint = ConstraintDescriptorFactory.uniqueForLabel( labelId, propertyKey );
        byte[] bytes = Base64.getDecoder().decode( serialized );

        // WHEN
        ConstraintRule deserialized = assertConstraintRule( SchemaRuleSerialization.deserialize( ruleId, ByteBuffer.wrap( bytes ) ) );

        // THEN
        assertThat( deserialized.getId(), equalTo( ruleId ) );
        assertThat( deserialized.getConstraintDescriptor(), equalTo( constraint ) );
        assertThat( deserialized.getSchemaDescriptor(), equalTo( constraint.schema() ) );
        assertThat( deserialized.getOwnedIndex(), equalTo( ownedIndexId ) );
    }

    private void assertParseNodePropertyExistsRule( String serialized ) throws Exception
    {
        // GIVEN
        long ruleId = 87;
        int propertyKey = 51;
        int labelId = 45;
        ConstraintDescriptor constraint = ConstraintDescriptorFactory.existsForLabel( labelId, propertyKey );
        byte[] bytes = Base64.getDecoder().decode( serialized );

        // WHEN
        ConstraintRule deserialized = assertConstraintRule( SchemaRuleSerialization.deserialize( ruleId, ByteBuffer.wrap( bytes ) ) );

        // THEN
        assertThat( deserialized.getId(), equalTo( ruleId ) );
        assertThat( deserialized.getConstraintDescriptor(), equalTo( constraint ) );
        assertThat( deserialized.getSchemaDescriptor(), equalTo( constraint.schema() ) );
        assertException( deserialized::getOwnedIndex, IllegalStateException.class, "" );
    }

    private void assertParseRelationshipPropertyExistsRule( String serialized ) throws Exception
    {
        // GIVEN
        long ruleId = 51;
        int propertyKey = 6119;
        int relTypeId = 8512;
        ConstraintDescriptor constraint = ConstraintDescriptorFactory.existsForRelType( relTypeId, propertyKey );
        byte[] bytes = Base64.getDecoder().decode( serialized );

        // WHEN
        ConstraintRule deserialized = assertConstraintRule( SchemaRuleSerialization.deserialize( ruleId, ByteBuffer.wrap( bytes ) ) );

        // THEN
        assertThat( deserialized.getId(), equalTo( ruleId ) );
        assertThat( deserialized.getConstraintDescriptor(), equalTo( constraint ) );
        assertThat( deserialized.getSchemaDescriptor(), equalTo( constraint.schema() ) );
        assertException( deserialized::getOwnedIndex, IllegalStateException.class, "" );
    }

    // HELPERS

    private void assertSerializeAndDeserializeIndexRule( IndexRule indexRule )
            throws MalformedSchemaRuleException
    {
        // GIVEN
        ByteBuffer buffer = ByteBuffer.allocate( VERY_LARGE_CAPACITY );

        // WHEN
        SchemaRuleSerialization.serialize( indexRule, buffer );
        buffer.flip();
        IndexRule deserialized = assertIndexRule( SchemaRuleSerialization.deserialize( indexRule.getId(), buffer ) );

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

    private void assertSerializeAndDeserializeConstraintRule( ConstraintRule constraintRule )
            throws MalformedSchemaRuleException
    {
        // GIVEN
        ByteBuffer buffer = ByteBuffer.allocate( VERY_LARGE_CAPACITY );

        // WHEN
        SchemaRuleSerialization.serialize( constraintRule, buffer );
        buffer.flip();
        ConstraintRule deserialized =
                assertConstraintRule( SchemaRuleSerialization.deserialize( constraintRule.getId(), buffer ) );

        // THEN
        assertThat( deserialized.getId(), equalTo( constraintRule.getId() ) );
        assertThat( deserialized.getConstraintDescriptor(), equalTo( constraintRule.getConstraintDescriptor() ) );
        assertThat( deserialized.getSchemaDescriptor(), equalTo( constraintRule.getSchemaDescriptor() ) );
    }

    private ConstraintRule assertConstraintRule( SchemaRule schemaRule )
    {
        if ( !(schemaRule instanceof ConstraintRule) )
        {
            fail( "Expected ConstraintRule, but got "+schemaRule.getClass().getSimpleName() );
        }
        return (ConstraintRule)schemaRule;
    }

    private void assertCorrectLength( IndexRule indexRule ) throws MalformedSchemaRuleException
    {
        // GIVEN
        ByteBuffer buffer = ByteBuffer.allocate( VERY_LARGE_CAPACITY );
        SchemaRuleSerialization.serialize( indexRule, buffer );

        // THEN
        assertThat( SchemaRuleSerialization.lengthOf( indexRule ), equalTo( buffer.position() ) );
    }

    private void assertCorrectLength( ConstraintRule constraintRule ) throws MalformedSchemaRuleException
    {
        // GIVEN
        ByteBuffer buffer = ByteBuffer.allocate( VERY_LARGE_CAPACITY );
        SchemaRuleSerialization.serialize( constraintRule, buffer );

        // THEN
        assertThat( SchemaRuleSerialization.lengthOf( constraintRule ), equalTo( buffer.position() ) );
    }
}

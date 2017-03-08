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
import java.util.Arrays;
import java.util.Base64;
import java.util.stream.IntStream;

import org.neo4j.kernel.api.exceptions.schema.MalformedSchemaRuleException;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.api.schema_new.constaints.ConstraintDescriptor;
import org.neo4j.kernel.api.schema_new.constaints.ConstraintDescriptorFactory;
import org.neo4j.kernel.api.schema_new.constaints.UniquenessConstraintDescriptor;
import org.neo4j.kernel.api.schema_new.index.NewIndexDescriptor;
import org.neo4j.kernel.api.schema_new.index.NewIndexDescriptorFactory;
import org.neo4j.storageengine.api.schema.SchemaRule;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
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

    ConstraintRule constraintCompositeLabel = ConstraintRule.constraintRule( RULE_ID,
            ConstraintDescriptorFactory.existsForLabel( LABEL_ID, PROPERTY_ID_1, PROPERTY_ID_2 ) );

    ConstraintRule constraintCompositeRelType = ConstraintRule.constraintRule( RULE_ID_2,
            ConstraintDescriptorFactory.existsForRelType( REL_TYPE_ID, PROPERTY_ID_1, PROPERTY_ID_2 ) );

    // INDEX RULES

    @Test
    public void rulesCreatedWithoutNameMustHaveComputedName() throws Exception
    {
        assertThat( indexRegular.getName(), is( "index_1" ) );
        assertThat( indexUnique.getName(), is( "index_2" ) );
        assertThat( indexCompositeRegular.getName(), is( "index_1" ) );
        assertThat( indexCompositeUnique.getName(), is( "index_2" ) );
        assertThat( indexBigComposite.getName(), is( "index_1" ) );
        assertThat( constraintExistsLabel.getName(), is( "constraint_1" ) );
        assertThat( constraintUniqueLabel.getName(), is( "constraint_2" ) );
        assertThat( constraintExistsRelType.getName(), is( "constraint_2" ) );
        assertThat( constraintCompositeLabel.getName(), is( "constraint_1" ) );
        assertThat( constraintCompositeRelType.getName(), is( "constraint_2" ) );

        assertThat( SchemaRule.generateName( indexRegular ), is( "index_1" ) );
        assertThat( SchemaRule.generateName( indexUnique ), is( "index_2" ) );
        assertThat( SchemaRule.generateName( indexCompositeRegular ), is( "index_1" ) );
        assertThat( SchemaRule.generateName( indexCompositeUnique ), is( "index_2" ) );
        assertThat( SchemaRule.generateName( indexBigComposite ), is( "index_1" ) );
        assertThat( SchemaRule.generateName( constraintExistsLabel ), is( "constraint_1" ) );
        assertThat( SchemaRule.generateName( constraintUniqueLabel ), is( "constraint_2" ) );
        assertThat( SchemaRule.generateName( constraintExistsRelType ), is( "constraint_2" ) );
        assertThat( SchemaRule.generateName( constraintCompositeLabel ), is( "constraint_1" ) );
        assertThat( SchemaRule.generateName( constraintCompositeRelType ), is( "constraint_2" ) );
    }

    @Test
    public void rulesCreatedWithoutNameMustRetainComputedNameAfterDeserialisation() throws Exception
    {
        assertThat( serialiseAndDeserialise( indexRegular ).getName(), is( "index_1" ) );
        assertThat( serialiseAndDeserialise( indexUnique ).getName(), is( "index_2" ) );
        assertThat( serialiseAndDeserialise( indexCompositeRegular ).getName(), is( "index_1" ) );
        assertThat( serialiseAndDeserialise( indexCompositeUnique ).getName(), is( "index_2" ) );
        assertThat( serialiseAndDeserialise( indexBigComposite ).getName(), is( "index_1" ) );
        assertThat( serialiseAndDeserialise( constraintExistsLabel ).getName(), is( "constraint_1" ) );
        assertThat( serialiseAndDeserialise( constraintUniqueLabel ).getName(), is( "constraint_2" ) );
        assertThat( serialiseAndDeserialise( constraintExistsRelType ).getName(), is( "constraint_2" ) );
        assertThat( serialiseAndDeserialise( constraintCompositeLabel ).getName(), is( "constraint_1" ) );
        assertThat( serialiseAndDeserialise( constraintCompositeRelType ).getName(), is( "constraint_2" ) );
    }

    @Test
    public void rulesCreatedWithNameMustRetainGivenNameAfterDeserialisation() throws Exception
    {
        String name = "custom_rule";

        assertTrue( indexRegular.setName( name ) );
        assertThat( serialiseAndDeserialise( indexRegular ).getName(), is( name ) );
        assertTrue( indexUnique.setName( name ) );
        assertThat( serialiseAndDeserialise( indexUnique ).getName(), is( name ) );
        assertTrue( indexCompositeRegular.setName( name ) );
        assertThat( serialiseAndDeserialise( indexCompositeRegular ).getName(), is( name ) );
        assertTrue( indexCompositeUnique.setName( name ) );
        assertThat( serialiseAndDeserialise( indexCompositeUnique ).getName(), is( name ) );
        assertTrue( indexBigComposite.setName( name ) );
        assertThat( serialiseAndDeserialise( indexBigComposite ).getName(), is( name ) );
        assertTrue( constraintExistsLabel.setName( name ) );
        assertThat( serialiseAndDeserialise( constraintExistsLabel ).getName(), is( name ) );
        assertTrue( constraintUniqueLabel.setName( name ) );
        assertThat( serialiseAndDeserialise( constraintUniqueLabel ).getName(), is( name ) );
        assertTrue( constraintExistsRelType.setName( name ) );
        assertThat( serialiseAndDeserialise( constraintExistsRelType ).getName(), is( name ) );
        assertTrue( constraintCompositeLabel.setName( name ) );
        assertThat( serialiseAndDeserialise( constraintCompositeLabel ).getName(), is( name ) );
        assertTrue( constraintCompositeRelType.setName( name ) );
        assertThat( serialiseAndDeserialise( constraintCompositeRelType ).getName(), is( name ) );
    }

    @Test
    public void settingNameOfUnnamedRuleToNullMustHaveNoEffect() throws Exception
    {
        assertTrue( indexRegular.setName( null ) );
        assertTrue( indexRegular.setName( null ) );
        assertTrue( indexRegular.setName( null ) );
        assertTrue( indexRegular.setName( "not null" ) );
        assertFalse( indexRegular.setName( "not null" ) );
        assertFalse( indexRegular.setName( "also not null" ) );
        assertFalse( indexRegular.setName( null ) );
    }

    @Test( expected = IllegalArgumentException.class )
    public void nameMustNotContainNullCharacter() throws Exception
    {
        String name = "a\0b";
        indexRegular.setName( name );
    }

    @Test( expected = IllegalArgumentException.class )
    public void nameMustNotBeTheEmptyString() throws Exception
    {
        //noinspection RedundantStringConstructorCall
        indexRegular.setName( new String( "" ) );
    }

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
    public void mustNotBeAbleToSetNameOfUnnamedDeserialisedIndexRule() throws Exception
    {
        byte[] bytes = decodeBase64( "/////wsAAAAOaW5kZXgtcHJvdmlkZXIAAAAEMjUuMB9bAAACAAABAAAABA==" );
        IndexRule rule = assertIndexRule( SchemaRuleSerialization.deserialize( 24, ByteBuffer.wrap( bytes ) ) );
        assertFalse( "should not be able to set name of deserialised rule", rule.setName( "my_rule" ) );
        assertThat( rule.getName(), is( "index_24" ) );
    }

    @Test
    public void mustNotBeAbleToSetNameOfUnnamedDeserialisedConstraintRule() throws Exception
    {
        byte[] bytes = decodeBase64( "/////ww9WwAAAC0AAQAAADM=" );
        ConstraintRule rule = assertConstraintRule( SchemaRuleSerialization.deserialize( 87, ByteBuffer.wrap( bytes ) ) );
        assertFalse( "should not be able to set name of deserialised rule", rule.setName( "my_rule" ) );
        assertThat( rule.getName(), is( "constraint_87" ) );
    }

    @Test
    public void shouldParseIndexRule() throws Exception
    {
        assertParseIndexRule( "/////wsAAAAOaW5kZXgtcHJvdmlkZXIAAAAEMjUuMB9bAAACAAABAAAABA==", "index_24" );
        assertParseIndexRule( "AAACAAEAAAAOaW5kZXgtcHJvdmlkZXIAAAAEMjUuMAABAAAAAAAAAAQ=", "index_24" ); // LEGACY
        assertParseIndexRule( "/////wsAAAAOaW5kZXgtcHJvdmlkZXIAAAAEMjUuMB9bAAACAAABAAAABGN1c3RvbV9uYW1lAA==", "custom_name" ); // named rule
        assertParseIndexRule( addNullByte( "/////wsAAAAOaW5kZXgtcHJvdmlkZXIAAAAEMjUuMB9bAAACAAABAAAABA==" ), "index_24" ); // empty name
    }

    @Test
    public void shouldParseUniqueIndexRule() throws Exception
    {
        assertParseUniqueIndexRule( "/////wsAAAAOaW5kZXgtcHJvdmlkZXIAAAAEMjUuMCAAAAAAAAAAC1sAAAA9AAEAAAPc", "index_33" );
        assertParseUniqueIndexRule( "AAAAPQIAAAAOaW5kZXgtcHJvdmlkZXIAAAAEMjUuMAABAAAAAAAAA9wAAAAAAAAACw==", "index_33" ); // LEGACY
        assertParseUniqueIndexRule( "/////wsAAAAOaW5kZXgtcHJvdmlkZXIAAAAEMjUuMCAAAAAAAAAAC1sAAAA9AAEAAAPcY3VzdG9tX25hbWUA", "custom_name" ); // named rule
        assertParseUniqueIndexRule( addNullByte( "/////wsAAAAOaW5kZXgtcHJvdmlkZXIAAAAEMjUuMCAAAAAAAAAAC1sAAAA9AAEAAAPc" ), "index_33" ); // empty name
    }

    @Test
    public void shouldParseUniqueConstraintRule() throws Exception
    {
        assertParseUniqueConstraintRule( "/////ww+AAAAAAAAAAJbAAAANwABAAAAAw==", "constraint_1" );
        assertParseUniqueConstraintRule( "AAAANwMBAAAAAAAAAAMAAAAAAAAAAg==", "constraint_1" ); // LEGACY
        assertParseUniqueConstraintRule( "/////ww+AAAAAAAAAAJbAAAANwABAAAAA2N1c3RvbV9uYW1lAA==", "custom_name" ); // named rule
        assertParseUniqueConstraintRule( addNullByte( "/////ww+AAAAAAAAAAJbAAAANwABAAAAAw==" ), "constraint_1" ); // empty name
    }

    @Test
    public void shouldParseNodePropertyExistsRule() throws Exception
    {
        assertParseNodePropertyExistsRule( "/////ww9WwAAAC0AAQAAADM=", "constraint_87" );
        assertParseNodePropertyExistsRule( "AAAALQQAAAAz", "constraint_87" ); // LEGACY
        assertParseNodePropertyExistsRule( "/////ww9WwAAAC0AAQAAADNjdXN0b21fbmFtZQA=", "custom_name" ); // named rule
        assertParseNodePropertyExistsRule( addNullByte( "/////ww9WwAAAC0AAQAAADM=" ), "constraint_87" ); // empty name
    }

    @Test
    public void shouldParseRelationshipPropertyExistsRule() throws Exception
    {
        assertParseRelationshipPropertyExistsRule( "/////ww9XAAAIUAAAQAAF+c=", "constraint_51" );
        assertParseRelationshipPropertyExistsRule( "AAAhQAUAABfn", "constraint_51" ); // LEGACY6
        assertParseRelationshipPropertyExistsRule( "/////ww9XAAAIUAAAQAAF+djdXN0b21fbmFtZQA=", "custom_name" ); // named rule
        assertParseRelationshipPropertyExistsRule( addNullByte( "/////ww9XAAAIUAAAQAAF+c=" ), "constraint_51" ); // empty name
    }

    private void assertParseIndexRule( String serialized, String name ) throws Exception
    {
        // GIVEN
        long ruleId = 24;
        NewIndexDescriptor index = NewIndexDescriptorFactory.forLabel( 512, 4 );
        SchemaIndexProvider.Descriptor indexProvider = new SchemaIndexProvider.Descriptor( "index-provider", "25.0" );
        byte[] bytes = decodeBase64( serialized );

        // WHEN
        IndexRule deserialized = assertIndexRule( SchemaRuleSerialization.deserialize( ruleId, ByteBuffer.wrap( bytes ) ) );

        // THEN
        assertThat( deserialized.getId(), equalTo( ruleId ) );
        assertThat( deserialized.getIndexDescriptor(), equalTo( index ) );
        assertThat( deserialized.schema(), equalTo( index.schema() ) );
        assertThat( deserialized.getProviderDescriptor(), equalTo( indexProvider ) );
        assertThat( deserialized.getName(), is( name ) );
        assertException( deserialized::getOwningConstraint, IllegalStateException.class, "" );
    }

    private void assertParseUniqueIndexRule( String serialized, String name ) throws MalformedSchemaRuleException
    {
        // GIVEN
        long ruleId = 33;
        long constraintId = 11;
        NewIndexDescriptor index = NewIndexDescriptorFactory.uniqueForLabel( 61, 988 );
        SchemaIndexProvider.Descriptor indexProvider = new SchemaIndexProvider.Descriptor( "index-provider", "25.0" );
        byte[] bytes = decodeBase64( serialized );

        // WHEN
        IndexRule deserialized = assertIndexRule( SchemaRuleSerialization.deserialize( ruleId, ByteBuffer.wrap( bytes ) ) );

        // THEN
        assertThat( deserialized.getId(), equalTo( ruleId ) );
        assertThat( deserialized.getIndexDescriptor(), equalTo( index ) );
        assertThat( deserialized.schema(), equalTo( index.schema() ) );
        assertThat( deserialized.getProviderDescriptor(), equalTo( indexProvider ) );
        assertThat( deserialized.getOwningConstraint(), equalTo( constraintId ) );
        assertThat( deserialized.getName(), is( name ) );
    }

    private void assertParseUniqueConstraintRule( String serialized, String name ) throws MalformedSchemaRuleException
    {
        // GIVEN
        long ruleId = 1;
        int propertyKey = 3;
        int labelId = 55;
        long ownedIndexId = 2;
        UniquenessConstraintDescriptor constraint = ConstraintDescriptorFactory.uniqueForLabel( labelId, propertyKey );
        byte[] bytes = decodeBase64( serialized );

        // WHEN
        ConstraintRule deserialized = assertConstraintRule( SchemaRuleSerialization.deserialize( ruleId, ByteBuffer.wrap( bytes ) ) );

        // THEN
        assertThat( deserialized.getId(), equalTo( ruleId ) );
        assertThat( deserialized.getConstraintDescriptor(), equalTo( constraint ) );
        assertThat( deserialized.schema(), equalTo( constraint.schema() ) );
        assertThat( deserialized.getOwnedIndex(), equalTo( ownedIndexId ) );
        assertThat( deserialized.getName(), is( name ) );
    }

    private void assertParseNodePropertyExistsRule( String serialized, String name ) throws Exception
    {
        // GIVEN
        long ruleId = 87;
        int propertyKey = 51;
        int labelId = 45;
        ConstraintDescriptor constraint = ConstraintDescriptorFactory.existsForLabel( labelId, propertyKey );
        byte[] bytes = decodeBase64( serialized );

        // WHEN
        ConstraintRule deserialized = assertConstraintRule( SchemaRuleSerialization.deserialize( ruleId, ByteBuffer.wrap( bytes ) ) );

        // THEN
        assertThat( deserialized.getId(), equalTo( ruleId ) );
        assertThat( deserialized.getConstraintDescriptor(), equalTo( constraint ) );
        assertThat( deserialized.schema(), equalTo( constraint.schema() ) );
        assertException( deserialized::getOwnedIndex, IllegalStateException.class, "" );
        assertThat( deserialized.getName(), is( name ) );
    }

    private void assertParseRelationshipPropertyExistsRule( String serialized, String name ) throws Exception
    {
        // GIVEN
        long ruleId = 51;
        int propertyKey = 6119;
        int relTypeId = 8512;
        ConstraintDescriptor constraint = ConstraintDescriptorFactory.existsForRelType( relTypeId, propertyKey );
        byte[] bytes = decodeBase64( serialized );

        // WHEN
        ConstraintRule deserialized = assertConstraintRule( SchemaRuleSerialization.deserialize( ruleId, ByteBuffer.wrap( bytes ) ) );

        // THEN
        assertThat( deserialized.getId(), equalTo( ruleId ) );
        assertThat( deserialized.getConstraintDescriptor(), equalTo( constraint ) );
        assertThat( deserialized.schema(), equalTo( constraint.schema() ) );
        assertException( deserialized::getOwnedIndex, IllegalStateException.class, "" );
        assertThat( deserialized.getName(), is( name ) );
    }

    // HELPERS

    private void assertSerializeAndDeserializeIndexRule( IndexRule indexRule )
            throws MalformedSchemaRuleException
    {
        IndexRule deserialized = assertIndexRule( serialiseAndDeserialise( indexRule ) );

        assertThat( deserialized.getId(), equalTo( indexRule.getId() ) );
        assertThat( deserialized.getIndexDescriptor(), equalTo( indexRule.getIndexDescriptor() ) );
        assertThat( deserialized.schema(), equalTo( indexRule.schema() ) );
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
        ConstraintRule deserialized = assertConstraintRule( serialiseAndDeserialise( constraintRule ) );

        assertThat( deserialized.getId(), equalTo( constraintRule.getId() ) );
        assertThat( deserialized.getConstraintDescriptor(), equalTo( constraintRule.getConstraintDescriptor() ) );
        assertThat( deserialized.schema(), equalTo( constraintRule.schema() ) );
    }

    private SchemaRule serialiseAndDeserialise( ConstraintRule constraintRule ) throws MalformedSchemaRuleException
    {
        ByteBuffer buffer = ByteBuffer.wrap( constraintRule.serialize() );
        return SchemaRuleSerialization.deserialize( constraintRule.getId(), buffer );
    }

    private SchemaRule serialiseAndDeserialise( IndexRule indexRule ) throws MalformedSchemaRuleException
    {
        ByteBuffer buffer = ByteBuffer.wrap( indexRule.serialize() );
        return SchemaRuleSerialization.deserialize( indexRule.getId(), buffer );
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
        ByteBuffer buffer = ByteBuffer.wrap( indexRule.serialize() );

        // THEN
        assertThat( SchemaRuleSerialization.lengthOf( indexRule ), equalTo( buffer.capacity() ) );
    }

    private void assertCorrectLength( ConstraintRule constraintRule ) throws MalformedSchemaRuleException
    {
        // GIVEN
        ByteBuffer buffer = ByteBuffer.wrap( constraintRule.serialize() );

        // THEN
        assertThat( SchemaRuleSerialization.lengthOf( constraintRule ), equalTo( buffer.capacity() ) );
    }

    private byte[] decodeBase64( String serialized )
    {
        return Base64.getDecoder().decode( serialized );
    }

    private String encodeBase64( byte[] bytes )
    {
        return Base64.getEncoder().encodeToString( bytes );
    }

    /**
     * Used to append a null-byte to the end of the base64 input and return the resulting base64 output.
     * The reason we need this, is because the rule names are null-terminated strings at the end of the encoded
     * schema rules.
     * By appending a null-byte, we effectively an empty string as the rule name. However, this is not really an
     * allowed rule name, so when we deserialise these rules, we should get the generated rule name back.
     * This can potentially be used in the future in case we don't want to give a rule a name, but still want to put
     * fields after where the name would be.
     * In that case, a single null-byte would suffice to indicate that the name field is (almost) not there.
     */
    private String addNullByte( String input )
    {
        byte[] inputBytes = decodeBase64( input );
        byte[] outputBytes = Arrays.copyOf( inputBytes, inputBytes.length + 1 );
        return encodeBase64( outputBytes );
    }
}

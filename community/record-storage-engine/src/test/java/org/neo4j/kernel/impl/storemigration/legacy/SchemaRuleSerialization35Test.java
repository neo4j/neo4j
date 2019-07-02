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
package org.neo4j.kernel.impl.storemigration.legacy;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Base64;
import java.util.stream.IntStream;

import org.neo4j.common.EntityType;
import org.neo4j.internal.kernel.api.exceptions.schema.MalformedSchemaRuleException;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexConfig;
import org.neo4j.internal.schema.IndexDescriptor2;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory;
import org.neo4j.internal.schema.constraints.NodeKeyConstraintDescriptor;
import org.neo4j.internal.schema.constraints.UniquenessConstraintDescriptor;
import org.neo4j.storageengine.api.ConstraintRule;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.internal.schema.SchemaDescriptor.fulltext;
import static org.neo4j.test.assertion.Assert.assertException;

class SchemaRuleSerialization35Test
{
    private static final long RULE_ID = 1;
    private static final long RULE_ID_2 = 2;
    private static final int LABEL_ID = 10;
    private static final int LABEL_ID_2 = 11;
    private static final int REL_TYPE_ID = 20;
    private static final int PROPERTY_ID_1 = 30;
    private static final int PROPERTY_ID_2 = 31;

    private static final String PROVIDER_KEY = "index-provider";
    private static final String PROVIDER_VERSION = "1.0";
    protected static final IndexProviderDescriptor PROVIDER = new IndexProviderDescriptor( PROVIDER_KEY, PROVIDER_VERSION );

    private static IndexPrototype indexPrototype( boolean isUnique, String name, int labelId, int... propertyIds )
    {
        SchemaDescriptor schema = SchemaDescriptor.forLabel( labelId, propertyIds );
        return indexPrototype( isUnique, name, schema );
    }

    private static IndexPrototype indexPrototype( boolean isUnique, String name, SchemaDescriptor schema )
    {
        IndexPrototype prototype = isUnique ? IndexPrototype.uniqueForSchema( schema, PROVIDER ) : IndexPrototype.forSchema( schema, PROVIDER );
        if ( name != null )
        {
            prototype = prototype.withName( name );
        }
        return prototype;
    }

    private static IndexPrototype forLabel( int labelId, int... propertyIds )
    {
        return indexPrototype( false, null, labelId, propertyIds );
    }

    public static IndexPrototype namedForLabel( String name, int labelId, int... propertyIds )
    {
        return indexPrototype( false, name, labelId, propertyIds );
    }

    private static IndexPrototype uniqueForLabel( int labelId, int... propertyIds )
    {
        return indexPrototype( true, null, labelId, propertyIds );
    }

    private static IndexPrototype namedUniqueForLabel( String name, int labelId, int... propertyIds )
    {
        return indexPrototype( true, name, labelId, propertyIds );
    }

    static IndexDescriptor2 withId( IndexPrototype prototype, long id )
    {
        return prototype.materialise( id );
    }

    private static IndexDescriptor2 withIds( IndexPrototype prototype, long id, long owningConstraintId )
    {
        return withId( prototype, id ).withOwningConstraintId( owningConstraintId );
    }

    private final IndexDescriptor2 indexRegular = withId( forLabel( LABEL_ID, PROPERTY_ID_1 ), RULE_ID );

    private final IndexDescriptor2 indexUnique = withIds( uniqueForLabel( LABEL_ID, PROPERTY_ID_1 ), RULE_ID_2, RULE_ID );

    private final IndexDescriptor2 indexCompositeRegular = withId( forLabel( LABEL_ID, PROPERTY_ID_1, PROPERTY_ID_2 ), RULE_ID );

    private final IndexDescriptor2 indexMultiTokenRegular = withId(
            indexPrototype( false, null,
                    fulltext( EntityType.NODE, IndexConfig.empty(), new int[]{LABEL_ID, LABEL_ID_2}, new int[]{PROPERTY_ID_1, PROPERTY_ID_2} ) ),
            RULE_ID );

    private final IndexDescriptor2 indexCompositeUnique = withIds( uniqueForLabel( LABEL_ID, PROPERTY_ID_1, PROPERTY_ID_2 ), RULE_ID_2, RULE_ID );

    private final IndexDescriptor2 indexBigComposite = withId( forLabel( LABEL_ID, IntStream.range(1, 200).toArray() ), RULE_ID );

    private final IndexDescriptor2 indexBigMultiToken = withId(
            indexPrototype( false, null,
                    fulltext( EntityType.RELATIONSHIP, IndexConfig.empty(), IntStream.range( 1, 200 ).toArray(), IntStream.range( 1, 200 ).toArray() ) ),
            RULE_ID );

    private final ConstraintRule constraintExistsLabel = ConstraintRule.constraintRule( RULE_ID,
            ConstraintDescriptorFactory.existsForLabel( LABEL_ID, PROPERTY_ID_1 ) );

    private final ConstraintRule constraintUniqueLabel = ConstraintRule.constraintRule( RULE_ID_2,
            ConstraintDescriptorFactory.uniqueForLabel( LABEL_ID, PROPERTY_ID_1 ), RULE_ID );

    private final ConstraintRule constraintNodeKeyLabel = ConstraintRule.constraintRule( RULE_ID_2,
            ConstraintDescriptorFactory.nodeKeyForLabel( LABEL_ID, PROPERTY_ID_1 ), RULE_ID );

    private final ConstraintRule constraintExistsRelType = ConstraintRule.constraintRule( RULE_ID_2,
            ConstraintDescriptorFactory.existsForRelType( REL_TYPE_ID, PROPERTY_ID_1 ) );

    private final ConstraintRule constraintCompositeLabel = ConstraintRule.constraintRule( RULE_ID,
            ConstraintDescriptorFactory.existsForLabel( LABEL_ID, PROPERTY_ID_1, PROPERTY_ID_2 ) );

    private final ConstraintRule constraintCompositeRelType = ConstraintRule.constraintRule( RULE_ID_2,
            ConstraintDescriptorFactory.existsForRelType( REL_TYPE_ID, PROPERTY_ID_1, PROPERTY_ID_2 ) );

    // INDEX RULES

    @Test
    void rulesCreatedWithoutNameMustHaveComputedName()
    {
        assertThat( indexRegular.getName(), is( "index_1" ) );
        assertThat( indexUnique.getName(), is( "index_2" ) );
        assertThat( indexCompositeRegular.getName(), is( "index_1" ) );
        assertThat( indexCompositeUnique.getName(), is( "index_2" ) );
        assertThat( indexBigComposite.getName(), is( "index_1" ) );
        assertThat( constraintExistsLabel.getName(), is( "constraint_1" ) );
        assertThat( constraintUniqueLabel.getName(), is( "constraint_2" ) );
        assertThat( constraintNodeKeyLabel.getName(), is( "constraint_2" ) );
        assertThat( constraintExistsRelType.getName(), is( "constraint_2" ) );
        assertThat( constraintCompositeLabel.getName(), is( "constraint_1" ) );
        assertThat( constraintCompositeRelType.getName(), is( "constraint_2" ) );
    }

    @Test
    void rulesCreatedWithoutNameMustRetainComputedNameAfterDeserialisation() throws Exception
    {
        assertThat( serialiseAndDeserialise( indexRegular ).getName(), is( "index_1" ) );
        assertThat( serialiseAndDeserialise( indexUnique ).getName(), is( "index_2" ) );
        assertThat( serialiseAndDeserialise( indexCompositeRegular ).getName(), is( "index_1" ) );
        assertThat( serialiseAndDeserialise( indexCompositeUnique ).getName(), is( "index_2" ) );
        assertThat( serialiseAndDeserialise( indexBigComposite ).getName(), is( "index_1" ) );
        assertThat( serialiseAndDeserialise( constraintExistsLabel ).getName(), is( "constraint_1" ) );
        assertThat( serialiseAndDeserialise( constraintUniqueLabel ).getName(), is( "constraint_2" ) );
        assertThat( serialiseAndDeserialise( constraintNodeKeyLabel ).getName(), is( "constraint_2" ) );
        assertThat( serialiseAndDeserialise( constraintExistsRelType ).getName(), is( "constraint_2" ) );
        assertThat( serialiseAndDeserialise( constraintCompositeLabel ).getName(), is( "constraint_1" ) );
        assertThat( serialiseAndDeserialise( constraintCompositeRelType ).getName(), is( "constraint_2" ) );
    }

    @Test
    void rulesCreatedWithNameMustRetainGivenNameAfterDeserialisation() throws Exception
    {
        String name = "custom_rule";

        assertThat( serialiseAndDeserialise( withId( namedForLabel( name, LABEL_ID, PROPERTY_ID_1 ), RULE_ID ) ).getName(), is( name ) );
        assertThat( serialiseAndDeserialise( withIds( namedUniqueForLabel( name, LABEL_ID, PROPERTY_ID_1 ), RULE_ID_2, RULE_ID ) ).getName(), is( name ) );
        assertThat( serialiseAndDeserialise( withId( namedForLabel( name, LABEL_ID, PROPERTY_ID_1, PROPERTY_ID_2 ), RULE_ID ) ).getName(), is( name ) );
        assertThat( serialiseAndDeserialise( withIds( namedUniqueForLabel( name, LABEL_ID, PROPERTY_ID_1, PROPERTY_ID_2 ), RULE_ID_2, RULE_ID ) ).getName(),
                is( name ) );
        assertThat( serialiseAndDeserialise( withId( namedForLabel( name, LABEL_ID, IntStream.range(1, 200).toArray() ), RULE_ID ) ).getName(), is( name ) );
        assertThat( serialiseAndDeserialise( ConstraintRule.constraintRule( RULE_ID,
                ConstraintDescriptorFactory.existsForLabel( LABEL_ID, PROPERTY_ID_1 ), name ) ).getName(), is( name ) );
        assertThat( serialiseAndDeserialise( ConstraintRule.constraintRule( RULE_ID_2,
                ConstraintDescriptorFactory.uniqueForLabel( LABEL_ID, PROPERTY_ID_1 ), RULE_ID, name ) ).getName(), is( name ) );
        assertThat( serialiseAndDeserialise( ConstraintRule.constraintRule( RULE_ID_2,
                ConstraintDescriptorFactory.nodeKeyForLabel( LABEL_ID, PROPERTY_ID_1 ), RULE_ID, name ) ).getName(), is( name ) );
        assertThat( serialiseAndDeserialise( ConstraintRule.constraintRule( RULE_ID_2,
                ConstraintDescriptorFactory.existsForRelType( REL_TYPE_ID, PROPERTY_ID_1 ), name ) ).getName(), is( name ) );
        assertThat( serialiseAndDeserialise( ConstraintRule.constraintRule( RULE_ID,
                ConstraintDescriptorFactory.existsForLabel( LABEL_ID, PROPERTY_ID_1, PROPERTY_ID_2 ), name ) ).getName(), is( name ) );
        assertThat( serialiseAndDeserialise( ConstraintRule.constraintRule( RULE_ID_2,
                ConstraintDescriptorFactory.existsForRelType( REL_TYPE_ID, PROPERTY_ID_1, PROPERTY_ID_2 ),
                name ) ).getName(), is( name ) );
    }

    @Test
    void rulesCreatedWithNullNameMustRetainComputedNameAfterDeserialisation() throws Exception
    {
        assertThat( serialiseAndDeserialise( withId( forLabel( LABEL_ID, PROPERTY_ID_1 ), RULE_ID ) ).getName(), is( "index_1" ) );
        assertThat( serialiseAndDeserialise( withIds( uniqueForLabel( LABEL_ID, PROPERTY_ID_1 ), RULE_ID_2, RULE_ID ) ).getName(), is( "index_2" ) );
        assertThat( serialiseAndDeserialise( withId( forLabel( LABEL_ID, PROPERTY_ID_1, PROPERTY_ID_2 ), RULE_ID ) ).getName(), is( "index_1" ) );
        assertThat( serialiseAndDeserialise( withIds( uniqueForLabel( LABEL_ID, PROPERTY_ID_1, PROPERTY_ID_2 ), RULE_ID_2, RULE_ID ) ).getName(),
                is( "index_2" ) );
        assertThat( serialiseAndDeserialise( withId( forLabel( LABEL_ID, IntStream.range(1, 200).toArray() ), RULE_ID ) ).getName(), is( "index_1" ) );

        String name = null;
        assertThat( serialiseAndDeserialise( ConstraintRule.constraintRule( RULE_ID,
                ConstraintDescriptorFactory.existsForLabel( LABEL_ID, PROPERTY_ID_1 ), name ) ).getName(),
                is( "constraint_1" ) );
        assertThat( serialiseAndDeserialise( ConstraintRule.constraintRule( RULE_ID_2,
                ConstraintDescriptorFactory.uniqueForLabel( LABEL_ID, PROPERTY_ID_1 ), RULE_ID, name ) ).getName(),
                is( "constraint_2" ) );
        assertThat( serialiseAndDeserialise( ConstraintRule.constraintRule( RULE_ID_2,
                ConstraintDescriptorFactory.nodeKeyForLabel( LABEL_ID, PROPERTY_ID_1 ), RULE_ID, name ) ).getName(),
                is( "constraint_2" ) );
        assertThat( serialiseAndDeserialise( ConstraintRule.constraintRule( RULE_ID_2,
                ConstraintDescriptorFactory.existsForRelType( REL_TYPE_ID, PROPERTY_ID_1 ), name ) ).getName(),
                is( "constraint_2" ) );
        assertThat( serialiseAndDeserialise( ConstraintRule.constraintRule( RULE_ID,
                ConstraintDescriptorFactory.existsForLabel( LABEL_ID, PROPERTY_ID_1, PROPERTY_ID_2 ), name ) ).getName(),
                is( "constraint_1" ) );
        assertThat( serialiseAndDeserialise( ConstraintRule.constraintRule( RULE_ID_2,
                ConstraintDescriptorFactory.existsForRelType( REL_TYPE_ID, PROPERTY_ID_1, PROPERTY_ID_2 ), name ) ).getName(),
                is( "constraint_2" ) );
    }

    @Test
    void indexRuleNameMustNotContainNullCharacter()
    {
        String name = "a\0b";
        assertThrows( IllegalArgumentException.class, () -> withId( namedForLabel( name, LABEL_ID, PROPERTY_ID_1 ), RULE_ID ) );
    }

    @Test
    void indexRuleNameMustNotBeTheEmptyString()
    {
        //noinspection RedundantStringConstructorCall
        String name = new String( "" );
        assertThrows( IllegalArgumentException.class, () -> withId( namedForLabel( name, LABEL_ID, PROPERTY_ID_1 ), RULE_ID ) );
    }

    @Test
    void constraintIndexRuleNameMustNotContainNullCharacter()
    {
        String name = "a\0b";
        assertThrows( IllegalArgumentException.class, () -> withIds( namedUniqueForLabel( name, LABEL_ID, PROPERTY_ID_1 ), RULE_ID, RULE_ID_2 ) );
    }

    @Test
    void constraintIndexRuleNameMustNotBeTheEmptyString()
    {
        //noinspection RedundantStringConstructorCall
        String name = new String( "" );
        assertThrows( IllegalArgumentException.class, () -> withIds( namedUniqueForLabel( name, LABEL_ID, PROPERTY_ID_1 ), RULE_ID, RULE_ID_2 ) );
    }

    @Test
    void constraintRuleNameMustNotContainNullCharacter()
    {
        String name = "a\0b";
        assertThrows( IllegalArgumentException.class,
            () -> ConstraintRule.constraintRule( RULE_ID, ConstraintDescriptorFactory.existsForLabel( LABEL_ID, PROPERTY_ID_1 ), name ) );
    }

    @Test
    void constraintRuleNameMustNotBeTheEmptyString()
    {
        //noinspection RedundantStringConstructorCall
        String name = new String( "" );
        assertThrows( IllegalArgumentException.class,
            () -> ConstraintRule.constraintRule( RULE_ID, ConstraintDescriptorFactory.existsForLabel( LABEL_ID, PROPERTY_ID_1 ), name ) );
    }

    @Test
    void uniquenessConstraintRuleNameMustNotContainNullCharacter()
    {
        String name = "a\0b";
        assertThrows( IllegalArgumentException.class,
            () -> ConstraintRule.constraintRule( RULE_ID, ConstraintDescriptorFactory.uniqueForLabel( LABEL_ID, PROPERTY_ID_1 ), RULE_ID_2, name ) );
    }

    @Test
    void uniquenessConstraintRuleNameMustNotBeTheEmptyString()
    {
        assertThrows( IllegalArgumentException.class,
            () -> ConstraintRule.constraintRule( RULE_ID, ConstraintDescriptorFactory.uniqueForLabel( LABEL_ID, PROPERTY_ID_1 ), RULE_ID_2, "" ) );
    }

    @Test
    void nodeKeyConstraintRuleNameMustNotContainNullCharacter()
    {
        String name = "a\0b";
        assertThrows( IllegalArgumentException.class,
            () -> ConstraintRule.constraintRule( RULE_ID, ConstraintDescriptorFactory.nodeKeyForLabel( LABEL_ID, PROPERTY_ID_1 ), RULE_ID_2, name ) );
    }

    @Test
    void nodeKeyConstraintRuleNameMustNotBeTheEmptyString()
    {
        //noinspection RedundantStringConstructorCall
        String name = new String( "" );
        assertThrows( IllegalArgumentException.class,
            () -> ConstraintRule.constraintRule( RULE_ID, ConstraintDescriptorFactory.nodeKeyForLabel( LABEL_ID, PROPERTY_ID_1 ), RULE_ID_2, name ) );
    }

    @Test
    void shouldSerializeAndDeserializeIndexRules() throws MalformedSchemaRuleException
    {
        assertSerializeAndDeserializeIndexRule( indexRegular );
        assertSerializeAndDeserializeIndexRule( indexUnique );
    }

    @Test
    void shouldSerializeAndDeserializeCompositeIndexRules() throws MalformedSchemaRuleException
    {
        assertSerializeAndDeserializeIndexRule( indexCompositeRegular );
        assertSerializeAndDeserializeIndexRule( indexCompositeUnique );
    }

    @Test
    void shouldSerializeAndDeserialize_Big_CompositeIndexRules() throws MalformedSchemaRuleException
    {
        assertSerializeAndDeserializeIndexRule( indexBigComposite );
    }

    // CONSTRAINT RULES

    @Test
    void shouldSerializeAndDeserializeConstraintRules() throws MalformedSchemaRuleException
    {
        assertSerializeAndDeserializeConstraintRule( constraintExistsLabel );
        assertSerializeAndDeserializeConstraintRule( constraintUniqueLabel );
        assertSerializeAndDeserializeConstraintRule( constraintNodeKeyLabel );
        assertSerializeAndDeserializeConstraintRule( constraintExistsRelType );
    }

    @Test
    void shouldSerializeAndDeserializeCompositeConstraintRules() throws MalformedSchemaRuleException
    {
        assertSerializeAndDeserializeConstraintRule( constraintCompositeLabel );
        assertSerializeAndDeserializeConstraintRule( constraintCompositeRelType );
    }

    @Test
    void shouldSerializeAndDeserializeMultiTokenRules() throws MalformedSchemaRuleException
    {
        assertSerializeAndDeserializeIndexRule( indexMultiTokenRegular );
        assertSerializeAndDeserializeIndexRule( indexBigMultiToken );
    }

    @Test
    void shouldReturnCorrectLengthForIndexRules()
    {
        assertCorrectLength( indexRegular );
        assertCorrectLength( indexUnique );
        assertCorrectLength( indexCompositeRegular );
        assertCorrectLength( indexCompositeUnique );
        assertCorrectLength( indexBigComposite );
    }

    @Test
    void shouldReturnCorrectLengthForConstraintRules()
    {
        assertCorrectLength( constraintExistsLabel );
    }

    // BACKWARDS COMPATIBILITY

    @Test
    void shouldParseIndexRule() throws Exception
    {
        assertParseIndexRule( "/////wsAAAAOaW5kZXgtcHJvdmlkZXIAAAAEMjUuMB9bAAACAAABAAAABA==", "index_24" );
        assertParseIndexRule( "AAACAAEAAAAOaW5kZXgtcHJvdmlkZXIAAAAEMjUuMAABAAAAAAAAAAQ=", "index_24" ); // LEGACY
        assertParseIndexRule( "/////wsAAAAOaW5kZXgtcHJvdmlkZXIAAAAEMjUuMB9bAAACAAABAAAABAAAAAtjdXN0b21fbmFtZQ==",
                "custom_name" ); // named rule
        assertParseIndexRule( addNullByte( "/////wsAAAAOaW5kZXgtcHJvdmlkZXIAAAAEMjUuMB9bAAACAAABAAAABA==" ),
                "index_24" ); // empty name
        assertParseIndexRule( addNullByte( 2, "/////wsAAAAOaW5kZXgtcHJvdmlkZXIAAAAEMjUuMB9bAAACAAABAAAABA==" ),
                "index_24" ); // empty name
        assertParseIndexRule( addNullByte( 3, "/////wsAAAAOaW5kZXgtcHJvdmlkZXIAAAAEMjUuMB9bAAACAAABAAAABA==" ),
                "index_24" ); // empty name
        assertParseIndexRule( addNullByte( 4, "/////wsAAAAOaW5kZXgtcHJvdmlkZXIAAAAEMjUuMB9bAAACAAABAAAABA==" ),
                "index_24" ); // empty name
        assertParseIndexRule( addNullByte( 5, "/////wsAAAAOaW5kZXgtcHJvdmlkZXIAAAAEMjUuMB9bAAACAAABAAAABA==" ),
                "index_24" ); // empty name
    }

    @Test
    void shouldParseUniqueIndexRule() throws Exception
    {
        assertParseUniqueIndexRule( "/////wsAAAAOaW5kZXgtcHJvdmlkZXIAAAAEMjUuMCAAAAAAAAAAC1sAAAA9AAEAAAPc",
                "index_33" );
        assertParseUniqueIndexRule( "AAAAPQIAAAAOaW5kZXgtcHJvdmlkZXIAAAAEMjUuMAABAAAAAAAAA9wAAAAAAAAACw==",
                "index_33" ); // LEGACY
        assertParseUniqueIndexRule(
                "/////wsAAAAOaW5kZXgtcHJvdmlkZXIAAAAEMjUuMCAAAAAAAAAAC1sAAAA9AAEAAAPcAAAAC2N1c3RvbV9uYW1l",
                "custom_name" ); // named rule
        assertParseUniqueIndexRule(
                addNullByte( "/////wsAAAAOaW5kZXgtcHJvdmlkZXIAAAAEMjUuMCAAAAAAAAAAC1sAAAA9AAEAAAPc" ),
                "index_33" ); // empty name
        assertParseUniqueIndexRule(
                addNullByte( 2, "/////wsAAAAOaW5kZXgtcHJvdmlkZXIAAAAEMjUuMCAAAAAAAAAAC1sAAAA9AAEAAAPc" ),
                "index_33" ); // empty name
        assertParseUniqueIndexRule(
                addNullByte( 3, "/////wsAAAAOaW5kZXgtcHJvdmlkZXIAAAAEMjUuMCAAAAAAAAAAC1sAAAA9AAEAAAPc" ),
                "index_33" ); // empty name
        assertParseUniqueIndexRule(
                addNullByte( 4, "/////wsAAAAOaW5kZXgtcHJvdmlkZXIAAAAEMjUuMCAAAAAAAAAAC1sAAAA9AAEAAAPc" ),
                "index_33" ); // empty name
        assertParseUniqueIndexRule(
                addNullByte( 5, "/////wsAAAAOaW5kZXgtcHJvdmlkZXIAAAAEMjUuMCAAAAAAAAAAC1sAAAA9AAEAAAPc" ),
                "index_33" ); // empty name
    }

    @Test
    void shouldParseUniqueConstraintRule() throws Exception
    {
        assertParseUniqueConstraintRule( "/////ww+AAAAAAAAAAJbAAAANwABAAAAAw==", "constraint_1" );
        assertParseUniqueConstraintRule( "AAAANwMBAAAAAAAAAAMAAAAAAAAAAg==", "constraint_1" ); // LEGACY
        assertParseUniqueConstraintRule( "/////ww+AAAAAAAAAAJbAAAANwABAAAAAwAAAAtjdXN0b21fbmFtZQ==",
                "custom_name" ); // named rule
        assertParseUniqueConstraintRule( addNullByte( "/////ww+AAAAAAAAAAJbAAAANwABAAAAAw==" ),
                "constraint_1" ); // empty name
        assertParseUniqueConstraintRule( addNullByte( 2, "/////ww+AAAAAAAAAAJbAAAANwABAAAAAw==" ),
                "constraint_1" ); // empty name
        assertParseUniqueConstraintRule( addNullByte( 3, "/////ww+AAAAAAAAAAJbAAAANwABAAAAAw==" ),
                "constraint_1" ); // empty name
        assertParseUniqueConstraintRule( addNullByte( 4, "/////ww+AAAAAAAAAAJbAAAANwABAAAAAw==" ),
                "constraint_1" ); // empty name
        assertParseUniqueConstraintRule( addNullByte( 5, "/////ww+AAAAAAAAAAJbAAAANwABAAAAAw==" ),
                "constraint_1" ); // empty name
    }

    @Test
    void shouldParseNodeKeyConstraintRule() throws Exception
    {
        assertParseNodeKeyConstraintRule( "/////ww/AAAAAAAAAAJbAAAANwABAAAAAw==", "constraint_1" );
        assertParseNodeKeyConstraintRule( "/////ww/AAAAAAAAAAJbAAAANwABAAAAAwAAAAtjdXN0b21fbmFtZQ==",
                "custom_name" ); // named rule
        assertParseNodeKeyConstraintRule( addNullByte( "/////ww/AAAAAAAAAAJbAAAANwABAAAAAw==" ),
                "constraint_1" ); // empty name
        assertParseNodeKeyConstraintRule( addNullByte( 2, "/////ww/AAAAAAAAAAJbAAAANwABAAAAAw==" ),
                "constraint_1" ); // empty name
        assertParseNodeKeyConstraintRule( addNullByte( 3, "/////ww/AAAAAAAAAAJbAAAANwABAAAAAw==" ),
                "constraint_1" ); // empty name
        assertParseNodeKeyConstraintRule( addNullByte( 4, "/////ww/AAAAAAAAAAJbAAAANwABAAAAAw==" ),
                "constraint_1" ); // empty name
        assertParseNodeKeyConstraintRule( addNullByte( 5, "/////ww/AAAAAAAAAAJbAAAANwABAAAAAw==" ),
                "constraint_1" ); // empty name
    }

    @Test
    void shouldParseNodePropertyExistsRule() throws Exception
    {
        assertParseNodePropertyExistsRule( "/////ww9WwAAAC0AAQAAADM=", "constraint_87" );
        assertParseNodePropertyExistsRule( "AAAALQQAAAAz", "constraint_87" ); // LEGACY
        assertParseNodePropertyExistsRule( "/////ww9WwAAAC0AAQAAADMAAAALY3VzdG9tX25hbWU=",
                "custom_name" ); // named rule
        assertParseNodePropertyExistsRule( addNullByte( "/////ww9WwAAAC0AAQAAADM=" ), "constraint_87" ); // empty name
        assertParseNodePropertyExistsRule( addNullByte( 2, "/////ww9WwAAAC0AAQAAADM=" ),
                "constraint_87" ); // empty name
        assertParseNodePropertyExistsRule( addNullByte( 3, "/////ww9WwAAAC0AAQAAADM=" ),
                "constraint_87" ); // empty name
        assertParseNodePropertyExistsRule( addNullByte( 4, "/////ww9WwAAAC0AAQAAADM=" ),
                "constraint_87" ); // empty name
        assertParseNodePropertyExistsRule( addNullByte( 5, "/////ww9WwAAAC0AAQAAADM=" ),
                "constraint_87" ); // empty name
    }

    @Test
    void shouldParseRelationshipPropertyExistsRule() throws Exception
    {
        assertParseRelationshipPropertyExistsRule( "/////ww9XAAAIUAAAQAAF+c=", "constraint_51" );
        assertParseRelationshipPropertyExistsRule( "AAAhQAUAABfn", "constraint_51" ); // LEGACY6
        assertParseRelationshipPropertyExistsRule( "/////ww9XAAAIUAAAQAAF+cAAAALY3VzdG9tX25hbWU=",
                "custom_name" ); // named rule
        assertParseRelationshipPropertyExistsRule( addNullByte( "/////ww9XAAAIUAAAQAAF+c=" ),
                "constraint_51" ); // empty name
        assertParseRelationshipPropertyExistsRule( addNullByte( 2, "/////ww9XAAAIUAAAQAAF+c=" ),
                "constraint_51" ); // empty name
        assertParseRelationshipPropertyExistsRule( addNullByte( 3, "/////ww9XAAAIUAAAQAAF+c=" ),
                "constraint_51" ); // empty name
        assertParseRelationshipPropertyExistsRule( addNullByte( 4, "/////ww9XAAAIUAAAQAAF+c=" ),
                "constraint_51" ); // empty name
        assertParseRelationshipPropertyExistsRule( addNullByte( 5, "/////ww9XAAAIUAAAQAAF+c=" ),
                "constraint_51" ); // empty name
    }

    private static void assertParseIndexRule( String serialized, String name ) throws Exception
    {
        // GIVEN
        long ruleId = 24;
        IndexPrototype index = forLabel( 512, 4 );
        String providerVersion = "25.0";
        byte[] bytes = decodeBase64( serialized );

        // WHEN
        IndexDescriptor2 deserialized = assertIndexRule( SchemaRuleSerialization35.deserialize( ruleId, ByteBuffer.wrap( bytes ) ) );

        // THEN
        assertThat( deserialized.getId(), equalTo( ruleId ) );
        assertThat( deserialized, equalTo( index ) );
        assertThat( deserialized.schema(), equalTo( index.schema() ) );
        assertThat( deserialized.getIndexProvider().getKey(), equalTo( PROVIDER_KEY ) );
        assertThat( deserialized.getIndexProvider().getVersion(), equalTo( providerVersion ) );
        assertThat( deserialized.getName(), is( name ) );
        assertTrue( deserialized.getOwningConstraintId().isEmpty() );
    }

    private static void assertParseUniqueIndexRule( String serialized, String name ) throws MalformedSchemaRuleException
    {
        // GIVEN
        long ruleId = 33;
        long constraintId = 11;
        IndexPrototype index = uniqueForLabel( 61, 988 );
        String providerVersion = "25.0";
        byte[] bytes = decodeBase64( serialized );

        // WHEN
        IndexDescriptor2 deserialized = assertIndexRule( SchemaRuleSerialization35.deserialize( ruleId, ByteBuffer.wrap( bytes ) ) );

        // THEN
        assertThat( deserialized.getId(), equalTo( ruleId ) );
        assertThat( deserialized, equalTo( index ) );
        assertThat( deserialized.schema(), equalTo( index.schema() ) );
        assertThat( deserialized.getIndexProvider().getKey(), equalTo( PROVIDER_KEY ) );
        assertThat( deserialized.getIndexProvider().getVersion(), equalTo( providerVersion ) );
        assertThat( deserialized.getOwningConstraintId().isPresent(), equalTo( true ) );
        assertThat( deserialized.getOwningConstraintId().getAsLong(), equalTo( constraintId ) );
        assertThat( deserialized.getName(), is( name ) );
    }

    private static void assertParseUniqueConstraintRule( String serialized, String name ) throws MalformedSchemaRuleException
    {
        // GIVEN
        long ruleId = 1;
        int propertyKey = 3;
        int labelId = 55;
        long ownedIndexId = 2;
        UniquenessConstraintDescriptor constraint = ConstraintDescriptorFactory.uniqueForLabel( labelId, propertyKey );
        byte[] bytes = decodeBase64( serialized );

        // WHEN
        ConstraintRule deserialized = assertConstraintRule( SchemaRuleSerialization35.deserialize( ruleId, ByteBuffer.wrap( bytes ) ) );

        // THEN
        assertThat( deserialized.getId(), equalTo( ruleId ) );
        assertThat( deserialized.getConstraintDescriptor(), equalTo( constraint ) );
        assertThat( deserialized.schema(), equalTo( constraint.schema() ) );
        assertThat( deserialized.ownedIndexReference(), equalTo( ownedIndexId ) );
        assertThat( deserialized.getName(), is( name ) );
    }

    private static void assertParseNodeKeyConstraintRule( String serialized, String name ) throws MalformedSchemaRuleException
    {
        // GIVEN
        long ruleId = 1;
        int propertyKey = 3;
        int labelId = 55;
        long ownedIndexId = 2;
        NodeKeyConstraintDescriptor constraint = ConstraintDescriptorFactory.nodeKeyForLabel( labelId, propertyKey );
        byte[] bytes = decodeBase64( serialized );

        // WHEN
        ConstraintRule deserialized = assertConstraintRule( SchemaRuleSerialization35.deserialize( ruleId, ByteBuffer.wrap( bytes ) ) );

        // THEN
        assertThat( deserialized.getId(), equalTo( ruleId ) );
        assertThat( deserialized.getConstraintDescriptor(), equalTo( constraint ) );
        assertThat( deserialized.schema(), equalTo( constraint.schema() ) );
        assertThat( deserialized.ownedIndexReference(), equalTo( ownedIndexId ) );
        assertThat( deserialized.getName(), is( name ) );
    }

    private static void assertParseNodePropertyExistsRule( String serialized, String name ) throws Exception
    {
        // GIVEN
        long ruleId = 87;
        int propertyKey = 51;
        int labelId = 45;
        ConstraintDescriptor constraint = ConstraintDescriptorFactory.existsForLabel( labelId, propertyKey );
        byte[] bytes = decodeBase64( serialized );

        // WHEN
        ConstraintRule deserialized = assertConstraintRule( SchemaRuleSerialization35.deserialize( ruleId, ByteBuffer.wrap( bytes ) ) );

        // THEN
        assertThat( deserialized.getId(), equalTo( ruleId ) );
        assertThat( deserialized.getConstraintDescriptor(), equalTo( constraint ) );
        assertThat( deserialized.schema(), equalTo( constraint.schema() ) );
        assertException( deserialized::ownedIndexReference, IllegalStateException.class );
        assertThat( deserialized.getName(), is( name ) );
    }

    private static void assertParseRelationshipPropertyExistsRule( String serialized, String name ) throws Exception
    {
        // GIVEN
        long ruleId = 51;
        int propertyKey = 6119;
        int relTypeId = 8512;
        ConstraintDescriptor constraint = ConstraintDescriptorFactory.existsForRelType( relTypeId, propertyKey );
        byte[] bytes = decodeBase64( serialized );

        // WHEN
        ConstraintRule deserialized = assertConstraintRule( SchemaRuleSerialization35.deserialize( ruleId, ByteBuffer.wrap( bytes ) ) );

        // THEN
        assertThat( deserialized.getId(), equalTo( ruleId ) );
        assertThat( deserialized.getConstraintDescriptor(), equalTo( constraint ) );
        assertThat( deserialized.schema(), equalTo( constraint.schema() ) );
        assertException( deserialized::ownedIndexReference, IllegalStateException.class );
        assertThat( deserialized.getName(), is( name ) );
    }

    // HELPERS

    private static void assertSerializeAndDeserializeIndexRule( IndexDescriptor2 indexRule )
            throws MalformedSchemaRuleException
    {
        IndexDescriptor2 deserialized = assertIndexRule( serialiseAndDeserialise( indexRule ) );

        assertThat( deserialized.getId(), equalTo( indexRule.getId() ) );
        assertThat( deserialized, equalTo( indexRule ) );
        assertThat( deserialized.schema(), equalTo( indexRule.schema() ) );
        assertThat( deserialized.getIndexProvider(), equalTo( indexRule.getIndexProvider() ) );
    }

    private static IndexDescriptor2 assertIndexRule( SchemaRule schemaRule )
    {
        if ( !(schemaRule instanceof IndexDescriptor2) )
        {
            fail( "Expected IndexRule, but got " + schemaRule.getClass().getSimpleName() );
        }
        return (IndexDescriptor2)schemaRule;
    }

    private static void assertSerializeAndDeserializeConstraintRule( ConstraintRule constraintRule )
            throws MalformedSchemaRuleException
    {
        ConstraintRule deserialized = assertConstraintRule( serialiseAndDeserialise( constraintRule ) );

        assertThat( deserialized.getId(), equalTo( constraintRule.getId() ) );
        assertThat( deserialized.getConstraintDescriptor(), equalTo( constraintRule.getConstraintDescriptor() ) );
        assertThat( deserialized.schema(), equalTo( constraintRule.schema() ) );
    }

    private static SchemaRule serialiseAndDeserialise( ConstraintRule constraintRule ) throws MalformedSchemaRuleException
    {
        ByteBuffer buffer = ByteBuffer.wrap( SchemaRuleSerialization35.serialize( constraintRule ) );
        return SchemaRuleSerialization35.deserialize( constraintRule.getId(), buffer );
    }

    private static SchemaRule serialiseAndDeserialise( IndexDescriptor2 indexRule ) throws MalformedSchemaRuleException
    {
        ByteBuffer buffer = ByteBuffer.wrap( SchemaRuleSerialization35.serialize( indexRule ) );
        return SchemaRuleSerialization35.deserialize( indexRule.getId(), buffer );
    }

    private static ConstraintRule assertConstraintRule( SchemaRule schemaRule )
    {
        if ( !(schemaRule instanceof ConstraintRule) )
        {
            fail( "Expected ConstraintRule, but got " + schemaRule.getClass().getSimpleName() );
        }
        return (ConstraintRule)schemaRule;
    }

    private static void assertCorrectLength( IndexDescriptor2 indexRule )
    {
        // GIVEN
        ByteBuffer buffer = ByteBuffer.wrap( SchemaRuleSerialization35.serialize( indexRule ) );

        // THEN
        assertThat( SchemaRuleSerialization35.lengthOf( indexRule ), equalTo( buffer.capacity() ) );
    }

    private static void assertCorrectLength( ConstraintRule constraintRule )
    {
        // GIVEN
        ByteBuffer buffer = ByteBuffer.wrap( SchemaRuleSerialization35.serialize( constraintRule ) );

        // THEN
        assertThat( SchemaRuleSerialization35.lengthOf( constraintRule ), equalTo( buffer.capacity() ) );
    }

    private static byte[] decodeBase64( String serialized )
    {
        return Base64.getDecoder().decode( serialized );
    }

    private static String encodeBase64( byte[] bytes )
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
    private static String addNullByte( String input )
    {
        byte[] inputBytes = decodeBase64( input );
        byte[] outputBytes = Arrays.copyOf( inputBytes, inputBytes.length + 1 );
        return encodeBase64( outputBytes );
    }

    private static String addNullByte( int nullByteCountToAdd, String input )
    {
        if ( nullByteCountToAdd < 1 )
        {
            return input;
        }
        if ( nullByteCountToAdd == 1 )
        {
            return addNullByte( input );
        }
        return addNullByte( addNullByte( nullByteCountToAdd - 1, input ) );
    }
}

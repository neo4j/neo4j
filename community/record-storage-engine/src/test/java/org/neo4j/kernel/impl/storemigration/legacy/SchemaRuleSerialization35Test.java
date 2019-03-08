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

import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Base64;
import java.util.Optional;
import java.util.stream.IntStream;

import org.neo4j.common.EntityType;
import org.neo4j.internal.kernel.api.exceptions.schema.MalformedSchemaRuleException;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.DefaultIndexDescriptor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.SchemaDescriptorFactory;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory;
import org.neo4j.internal.schema.constraints.NodeKeyConstraintDescriptor;
import org.neo4j.internal.schema.constraints.UniquenessConstraintDescriptor;
import org.neo4j.storageengine.api.ConstraintRule;
import org.neo4j.storageengine.api.DefaultStorageIndexReference;
import org.neo4j.storageengine.api.StorageIndexReference;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.fail;
import static org.neo4j.internal.schema.SchemaDescriptorFactory.multiToken;
import static org.neo4j.test.assertion.Assert.assertException;

public class SchemaRuleSerialization35Test
{
    protected static final long RULE_ID = 1;
    protected static final long RULE_ID_2 = 2;
    protected static final int LABEL_ID = 10;
    protected static final int LABEL_ID_2 = 11;
    protected static final int REL_TYPE_ID = 20;
    protected static final int PROPERTY_ID_1 = 30;
    protected static final int PROPERTY_ID_2 = 31;

    protected static final String PROVIDER_KEY = "index-provider";
    protected static final String PROVIDER_VERSION = "1.0";

    private static IndexDescriptor indexDescriptor( boolean isUnique, String name, int labelId, int... propertyIds )
    {
        return new DefaultIndexDescriptor( SchemaDescriptorFactory.forLabel( labelId, propertyIds ), PROVIDER_KEY, PROVIDER_VERSION,
                Optional.ofNullable( name ), isUnique, false );
    }

    public static IndexDescriptor forLabel( int labelId, int... propertyIds )
    {
        return indexDescriptor( false, null, labelId, propertyIds );
    }

    public static IndexDescriptor namedForLabel( String name, int labelId, int... propertyIds )
    {
        return indexDescriptor( false, name, labelId, propertyIds );
    }

    public static IndexDescriptor uniqueForLabel( int labelId, int... propertyIds )
    {
        return indexDescriptor( true, null, labelId, propertyIds );
    }

    public static IndexDescriptor namedUniqueForLabel( String name, int labelId, int... propertyIds )
    {
        return indexDescriptor( true, name, labelId, propertyIds );
    }

    static StorageIndexReference withId( IndexDescriptor indexDescriptor, long id )
    {
        return new DefaultStorageIndexReference( indexDescriptor, id, null );
    }

    static StorageIndexReference withIds( IndexDescriptor indexDescriptor, long id, long owningConstraintId )
    {
        return new DefaultStorageIndexReference( indexDescriptor, id, owningConstraintId );
    }

    StorageIndexReference indexRegular = withId( forLabel( LABEL_ID, PROPERTY_ID_1 ), RULE_ID );

    StorageIndexReference indexUnique = withIds( uniqueForLabel( LABEL_ID, PROPERTY_ID_1 ), RULE_ID_2, RULE_ID );

    StorageIndexReference indexCompositeRegular = withId( forLabel( LABEL_ID, PROPERTY_ID_1, PROPERTY_ID_2 ), RULE_ID );

    StorageIndexReference indexMultiTokenRegular =
            withId( new DefaultIndexDescriptor( multiToken( new int[]{LABEL_ID, LABEL_ID_2}, EntityType.NODE, PROPERTY_ID_1, PROPERTY_ID_2 ), PROVIDER_KEY,
                    PROVIDER_VERSION, Optional.empty(), false, false ), RULE_ID );

    StorageIndexReference indexCompositeUnique = withIds( uniqueForLabel( LABEL_ID, PROPERTY_ID_1, PROPERTY_ID_2 ), RULE_ID_2, RULE_ID );

    StorageIndexReference indexBigComposite = withId( forLabel( LABEL_ID, IntStream.range(1, 200).toArray() ), RULE_ID );

    StorageIndexReference indexBigMultiToken =
            withId( new DefaultIndexDescriptor( multiToken( IntStream.range( 1, 200 ).toArray(), EntityType.RELATIONSHIP, IntStream.range( 1, 200 ).toArray() ),
                    PROVIDER_KEY, PROVIDER_VERSION, Optional.empty(), false, false ), RULE_ID );

    ConstraintRule constraintExistsLabel = ConstraintRule.constraintRule( RULE_ID,
            ConstraintDescriptorFactory.existsForLabel( LABEL_ID, PROPERTY_ID_1 ) );

    ConstraintRule constraintUniqueLabel = ConstraintRule.constraintRule( RULE_ID_2,
            ConstraintDescriptorFactory.uniqueForLabel( LABEL_ID, PROPERTY_ID_1 ), RULE_ID );

    ConstraintRule constraintNodeKeyLabel = ConstraintRule.constraintRule( RULE_ID_2,
            ConstraintDescriptorFactory.nodeKeyForLabel( LABEL_ID, PROPERTY_ID_1 ), RULE_ID );

    ConstraintRule constraintExistsRelType = ConstraintRule.constraintRule( RULE_ID_2,
            ConstraintDescriptorFactory.existsForRelType( REL_TYPE_ID, PROPERTY_ID_1 ) );

    ConstraintRule constraintCompositeLabel = ConstraintRule.constraintRule( RULE_ID,
            ConstraintDescriptorFactory.existsForLabel( LABEL_ID, PROPERTY_ID_1, PROPERTY_ID_2 ) );

    ConstraintRule constraintCompositeRelType = ConstraintRule.constraintRule( RULE_ID_2,
            ConstraintDescriptorFactory.existsForRelType( REL_TYPE_ID, PROPERTY_ID_1, PROPERTY_ID_2 ) );

    // INDEX RULES

    @Test
    public void rulesCreatedWithoutNameMustHaveComputedName()
    {
        assertThat( indexRegular.name(), is( "index_1" ) );
        assertThat( indexUnique.name(), is( "index_2" ) );
        assertThat( indexCompositeRegular.name(), is( "index_1" ) );
        assertThat( indexCompositeUnique.name(), is( "index_2" ) );
        assertThat( indexBigComposite.name(), is( "index_1" ) );
        assertThat( constraintExistsLabel.name(), is( "constraint_1" ) );
        assertThat( constraintUniqueLabel.name(), is( "constraint_2" ) );
        assertThat( constraintNodeKeyLabel.name(), is( "constraint_2" ) );
        assertThat( constraintExistsRelType.name(), is( "constraint_2" ) );
        assertThat( constraintCompositeLabel.name(), is( "constraint_1" ) );
        assertThat( constraintCompositeRelType.name(), is( "constraint_2" ) );
    }

    @Test
    public void rulesCreatedWithoutNameMustRetainComputedNameAfterDeserialisation() throws Exception
    {
        assertThat( serialiseAndDeserialise( indexRegular ).name(), is( "index_1" ) );
        assertThat( serialiseAndDeserialise( indexUnique ).name(), is( "index_2" ) );
        assertThat( serialiseAndDeserialise( indexCompositeRegular ).name(), is( "index_1" ) );
        assertThat( serialiseAndDeserialise( indexCompositeUnique ).name(), is( "index_2" ) );
        assertThat( serialiseAndDeserialise( indexBigComposite ).name(), is( "index_1" ) );
        assertThat( serialiseAndDeserialise( constraintExistsLabel ).name(), is( "constraint_1" ) );
        assertThat( serialiseAndDeserialise( constraintUniqueLabel ).name(), is( "constraint_2" ) );
        assertThat( serialiseAndDeserialise( constraintNodeKeyLabel ).name(), is( "constraint_2" ) );
        assertThat( serialiseAndDeserialise( constraintExistsRelType ).name(), is( "constraint_2" ) );
        assertThat( serialiseAndDeserialise( constraintCompositeLabel ).name(), is( "constraint_1" ) );
        assertThat( serialiseAndDeserialise( constraintCompositeRelType ).name(), is( "constraint_2" ) );
    }

    @Test
    public void rulesCreatedWithNameMustRetainGivenNameAfterDeserialisation() throws Exception
    {
        String name = "custom_rule";

        assertThat( serialiseAndDeserialise( withId( namedForLabel( name, LABEL_ID, PROPERTY_ID_1 ), RULE_ID ) ).name(), is( name ) );
        assertThat( serialiseAndDeserialise( withIds( namedUniqueForLabel( name, LABEL_ID, PROPERTY_ID_1 ), RULE_ID_2, RULE_ID ) ).name(), is( name ) );
        assertThat( serialiseAndDeserialise( withId( namedForLabel( name, LABEL_ID, PROPERTY_ID_1, PROPERTY_ID_2 ), RULE_ID ) ).name(), is( name ) );
        assertThat( serialiseAndDeserialise( withIds( namedUniqueForLabel( name, LABEL_ID, PROPERTY_ID_1, PROPERTY_ID_2 ), RULE_ID_2, RULE_ID ) ).name(),
                is( name ) );
        assertThat( serialiseAndDeserialise( withId( namedForLabel( name, LABEL_ID, IntStream.range(1, 200).toArray() ), RULE_ID ) ).name(), is( name ) );
        assertThat( serialiseAndDeserialise( ConstraintRule.constraintRule( RULE_ID,
                ConstraintDescriptorFactory.existsForLabel( LABEL_ID, PROPERTY_ID_1 ), name ) ).name(), is( name ) );
        assertThat( serialiseAndDeserialise( ConstraintRule.constraintRule( RULE_ID_2,
                ConstraintDescriptorFactory.uniqueForLabel( LABEL_ID, PROPERTY_ID_1 ), RULE_ID, name ) ).name(), is( name ) );
        assertThat( serialiseAndDeserialise( ConstraintRule.constraintRule( RULE_ID_2,
                ConstraintDescriptorFactory.nodeKeyForLabel( LABEL_ID, PROPERTY_ID_1 ), RULE_ID, name ) ).name(), is( name ) );
        assertThat( serialiseAndDeserialise( ConstraintRule.constraintRule( RULE_ID_2,
                ConstraintDescriptorFactory.existsForRelType( REL_TYPE_ID, PROPERTY_ID_1 ), name ) ).name(), is( name ) );
        assertThat( serialiseAndDeserialise( ConstraintRule.constraintRule( RULE_ID,
                ConstraintDescriptorFactory.existsForLabel( LABEL_ID, PROPERTY_ID_1, PROPERTY_ID_2 ), name ) ).name(), is( name ) );
        assertThat( serialiseAndDeserialise( ConstraintRule.constraintRule( RULE_ID_2,
                ConstraintDescriptorFactory.existsForRelType( REL_TYPE_ID, PROPERTY_ID_1, PROPERTY_ID_2 ),
                name ) ).name(), is( name ) );
    }

    @Test
    public void rulesCreatedWithNullNameMustRetainComputedNameAfterDeserialisation() throws Exception
    {
        assertThat( serialiseAndDeserialise( withId( forLabel( LABEL_ID, PROPERTY_ID_1 ), RULE_ID ) ).name(), is( "index_1" ) );
        assertThat( serialiseAndDeserialise( withIds( uniqueForLabel( LABEL_ID, PROPERTY_ID_1 ), RULE_ID_2, RULE_ID ) ).name(), is( "index_2" ) );
        assertThat( serialiseAndDeserialise( withId( forLabel( LABEL_ID, PROPERTY_ID_1, PROPERTY_ID_2 ), RULE_ID ) ).name(), is( "index_1" ) );
        assertThat( serialiseAndDeserialise( withIds( uniqueForLabel( LABEL_ID, PROPERTY_ID_1, PROPERTY_ID_2 ), RULE_ID_2, RULE_ID ) ).name(),
                is( "index_2" ) );
        assertThat( serialiseAndDeserialise( withId( forLabel( LABEL_ID, IntStream.range(1, 200).toArray() ), RULE_ID ) ).name(), is( "index_1" ) );

        String name = null;
        assertThat( serialiseAndDeserialise( ConstraintRule.constraintRule( RULE_ID,
                ConstraintDescriptorFactory.existsForLabel( LABEL_ID, PROPERTY_ID_1 ), name ) ).name(),
                is( "constraint_1" ) );
        assertThat( serialiseAndDeserialise( ConstraintRule.constraintRule( RULE_ID_2,
                ConstraintDescriptorFactory.uniqueForLabel( LABEL_ID, PROPERTY_ID_1 ), RULE_ID, name ) ).name(),
                is( "constraint_2" ) );
        assertThat( serialiseAndDeserialise( ConstraintRule.constraintRule( RULE_ID_2,
                ConstraintDescriptorFactory.nodeKeyForLabel( LABEL_ID, PROPERTY_ID_1 ), RULE_ID, name ) ).name(),
                is( "constraint_2" ) );
        assertThat( serialiseAndDeserialise( ConstraintRule.constraintRule( RULE_ID_2,
                ConstraintDescriptorFactory.existsForRelType( REL_TYPE_ID, PROPERTY_ID_1 ), name ) ).name(),
                is( "constraint_2" ) );
        assertThat( serialiseAndDeserialise( ConstraintRule.constraintRule( RULE_ID,
                ConstraintDescriptorFactory.existsForLabel( LABEL_ID, PROPERTY_ID_1, PROPERTY_ID_2 ), name ) ).name(),
                is( "constraint_1" ) );
        assertThat( serialiseAndDeserialise( ConstraintRule.constraintRule( RULE_ID_2,
                ConstraintDescriptorFactory.existsForRelType( REL_TYPE_ID, PROPERTY_ID_1, PROPERTY_ID_2 ), name ) ).name(),
                is( "constraint_2" ) );
    }

    @Test( expected = IllegalArgumentException.class )
    public void indexRuleNameMustNotContainNullCharacter()
    {
        String name = "a\0b";
        withId( namedForLabel( name, LABEL_ID, PROPERTY_ID_1 ), RULE_ID );
    }

    @Test( expected = IllegalArgumentException.class )
    public void indexRuleNameMustNotBeTheEmptyString()
    {
        //noinspection RedundantStringConstructorCall
        String name = new String( "" );
        withId( namedForLabel( name, LABEL_ID, PROPERTY_ID_1 ), RULE_ID );
    }

    @Test( expected = IllegalArgumentException.class )
    public void constraintIndexRuleNameMustNotContainNullCharacter()
    {
        String name = "a\0b";
        withIds( namedUniqueForLabel( name, LABEL_ID, PROPERTY_ID_1 ), RULE_ID, RULE_ID_2 );
    }

    @Test( expected = IllegalArgumentException.class )
    public void constraintIndexRuleNameMustNotBeTheEmptyString()
    {
        //noinspection RedundantStringConstructorCall
        String name = new String( "" );
        withIds( namedUniqueForLabel( name, LABEL_ID, PROPERTY_ID_1 ), RULE_ID, RULE_ID_2 );
    }

    @Test( expected = IllegalArgumentException.class )
    public void constraintRuleNameMustNotContainNullCharacter()
    {
        String name = "a\0b";
        ConstraintRule.constraintRule( RULE_ID,
                ConstraintDescriptorFactory.existsForLabel( LABEL_ID, PROPERTY_ID_1 ), name );
    }

    @Test( expected = IllegalArgumentException.class )
    public void constraintRuleNameMustNotBeTheEmptyString()
    {
        //noinspection RedundantStringConstructorCall
        String name = new String( "" );
        ConstraintRule.constraintRule( RULE_ID,
                ConstraintDescriptorFactory.existsForLabel( LABEL_ID, PROPERTY_ID_1 ), name );
    }

    @Test( expected = IllegalArgumentException.class )
    public void uniquenessConstraintRuleNameMustNotContainNullCharacter()
    {
        String name = "a\0b";
        ConstraintRule.constraintRule( RULE_ID,
                ConstraintDescriptorFactory.uniqueForLabel( LABEL_ID, PROPERTY_ID_1 ), RULE_ID_2, name );
    }

    @Test( expected = IllegalArgumentException.class )
    public void uniquenessConstraintRuleNameMustNotBeTheEmptyString()
    {
        //noinspection RedundantStringConstructorCall
        String name = new String( "" );
        ConstraintRule.constraintRule( RULE_ID,
                ConstraintDescriptorFactory.uniqueForLabel( LABEL_ID, PROPERTY_ID_1 ), RULE_ID_2, name );
    }

    @Test( expected = IllegalArgumentException.class )
    public void nodeKeyConstraintRuleNameMustNotContainNullCharacter()
    {
        String name = "a\0b";
        ConstraintRule.constraintRule( RULE_ID,
                ConstraintDescriptorFactory.nodeKeyForLabel( LABEL_ID, PROPERTY_ID_1 ), RULE_ID_2, name );
    }

    @Test( expected = IllegalArgumentException.class )
    public void nodeKeyConstraintRuleNameMustNotBeTheEmptyString()
    {
        //noinspection RedundantStringConstructorCall
        String name = new String( "" );
        ConstraintRule.constraintRule( RULE_ID,
                ConstraintDescriptorFactory.nodeKeyForLabel( LABEL_ID, PROPERTY_ID_1 ), RULE_ID_2, name );
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
        assertSerializeAndDeserializeConstraintRule( constraintNodeKeyLabel );
        assertSerializeAndDeserializeConstraintRule( constraintExistsRelType );
    }

    @Test
    public void shouldSerializeAndDeserializeCompositeConstraintRules() throws MalformedSchemaRuleException
    {
        assertSerializeAndDeserializeConstraintRule( constraintCompositeLabel );
        assertSerializeAndDeserializeConstraintRule( constraintCompositeRelType );
    }

    @Test
    public void shouldSerializeAndDeserializeMultiTokenRules() throws MalformedSchemaRuleException
    {
        assertSerializeAndDeserializeIndexRule( indexMultiTokenRegular );
        assertSerializeAndDeserializeIndexRule( indexBigMultiToken );
    }

    @Test
    public void shouldReturnCorrectLengthForIndexRules()
    {
        assertCorrectLength( indexRegular );
        assertCorrectLength( indexUnique );
        assertCorrectLength( indexCompositeRegular );
        assertCorrectLength( indexCompositeUnique );
        assertCorrectLength( indexBigComposite );
    }

    @Test
    public void shouldReturnCorrectLengthForConstraintRules()
    {
        assertCorrectLength( constraintExistsLabel );
    }

    // BACKWARDS COMPATIBILITY

    @Test
    public void shouldParseIndexRule() throws Exception
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
    public void shouldParseUniqueIndexRule() throws Exception
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
    public void shouldParseUniqueConstraintRule() throws Exception
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
    public void shouldParseNodeKeyConstraintRule() throws Exception
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
    public void shouldParseNodePropertyExistsRule() throws Exception
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
    public void shouldParseRelationshipPropertyExistsRule() throws Exception
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

    private void assertParseIndexRule( String serialized, String name ) throws Exception
    {
        // GIVEN
        long ruleId = 24;
        IndexDescriptor index = forLabel( 512, 4 );
        String providerVersion = "25.0";
        byte[] bytes = decodeBase64( serialized );

        // WHEN
        StorageIndexReference deserialized = assertIndexRule( SchemaRuleSerialization35.deserialize( ruleId, ByteBuffer.wrap( bytes ) ) );

        // THEN
        assertThat( deserialized.getId(), equalTo( ruleId ) );
        assertThat( deserialized, equalTo( index ) );
        assertThat( deserialized.schema(), equalTo( index.schema() ) );
        assertThat( deserialized.providerKey(), equalTo( PROVIDER_KEY ) );
        assertThat( deserialized.providerVersion(), equalTo( providerVersion ) );
        assertThat( deserialized.name(), is( name ) );
        assertException( deserialized::hasOwningConstraintReference, IllegalStateException.class );
        assertException( deserialized::owningConstraintReference, IllegalStateException.class );
    }

    private void assertParseUniqueIndexRule( String serialized, String name ) throws MalformedSchemaRuleException
    {
        // GIVEN
        long ruleId = 33;
        long constraintId = 11;
        IndexDescriptor index = uniqueForLabel( 61, 988 );
        String providerVersion = "25.0";
        byte[] bytes = decodeBase64( serialized );

        // WHEN
        StorageIndexReference deserialized = assertIndexRule( SchemaRuleSerialization35.deserialize( ruleId, ByteBuffer.wrap( bytes ) ) );

        // THEN
        assertThat( deserialized.getId(), equalTo( ruleId ) );
        assertThat( deserialized, equalTo( index ) );
        assertThat( deserialized.schema(), equalTo( index.schema() ) );
        assertThat( deserialized.providerKey(), equalTo( PROVIDER_KEY ) );
        assertThat( deserialized.providerVersion(), equalTo( providerVersion ) );
        assertThat( deserialized.hasOwningConstraintReference(), equalTo( true ) );
        assertThat( deserialized.owningConstraintReference(), equalTo( constraintId ) );
        assertThat( deserialized.name(), is( name ) );
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
        ConstraintRule deserialized = assertConstraintRule( SchemaRuleSerialization35.deserialize( ruleId, ByteBuffer.wrap( bytes ) ) );

        // THEN
        assertThat( deserialized.getId(), equalTo( ruleId ) );
        assertThat( deserialized.getConstraintDescriptor(), equalTo( constraint ) );
        assertThat( deserialized.schema(), equalTo( constraint.schema() ) );
        assertThat( deserialized.ownedIndexReference(), equalTo( ownedIndexId ) );
        assertThat( deserialized.name(), is( name ) );
    }

    private void assertParseNodeKeyConstraintRule( String serialized, String name ) throws MalformedSchemaRuleException
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
        assertThat( deserialized.name(), is( name ) );
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
        ConstraintRule deserialized = assertConstraintRule( SchemaRuleSerialization35.deserialize( ruleId, ByteBuffer.wrap( bytes ) ) );

        // THEN
        assertThat( deserialized.getId(), equalTo( ruleId ) );
        assertThat( deserialized.getConstraintDescriptor(), equalTo( constraint ) );
        assertThat( deserialized.schema(), equalTo( constraint.schema() ) );
        assertException( deserialized::ownedIndexReference, IllegalStateException.class );
        assertThat( deserialized.name(), is( name ) );
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
        ConstraintRule deserialized = assertConstraintRule( SchemaRuleSerialization35.deserialize( ruleId, ByteBuffer.wrap( bytes ) ) );

        // THEN
        assertThat( deserialized.getId(), equalTo( ruleId ) );
        assertThat( deserialized.getConstraintDescriptor(), equalTo( constraint ) );
        assertThat( deserialized.schema(), equalTo( constraint.schema() ) );
        assertException( deserialized::ownedIndexReference, IllegalStateException.class );
        assertThat( deserialized.name(), is( name ) );
    }

    // HELPERS

    private void assertSerializeAndDeserializeIndexRule( StorageIndexReference indexRule )
            throws MalformedSchemaRuleException
    {
        StorageIndexReference deserialized = assertIndexRule( serialiseAndDeserialise( indexRule ) );

        assertThat( deserialized.getId(), equalTo( indexRule.getId() ) );
        assertThat( deserialized, equalTo( indexRule ) );
        assertThat( deserialized.schema(), equalTo( indexRule.schema() ) );
        assertThat( deserialized.providerKey(), equalTo( indexRule.providerKey() ) );
        assertThat( deserialized.providerVersion(), equalTo( indexRule.providerVersion() ) );
    }

    private StorageIndexReference assertIndexRule( SchemaRule schemaRule )
    {
        if ( !(schemaRule instanceof StorageIndexReference) )
        {
            fail( "Expected IndexRule, but got " + schemaRule.getClass().getSimpleName() );
        }
        return (StorageIndexReference)schemaRule;
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
        ByteBuffer buffer = ByteBuffer.wrap( SchemaRuleSerialization35.serialize( constraintRule ) );
        return SchemaRuleSerialization35.deserialize( constraintRule.getId(), buffer );
    }

    private SchemaRule serialiseAndDeserialise( StorageIndexReference indexRule ) throws MalformedSchemaRuleException
    {
        ByteBuffer buffer = ByteBuffer.wrap( SchemaRuleSerialization35.serialize( indexRule ) );
        return SchemaRuleSerialization35.deserialize( indexRule.getId(), buffer );
    }

    private ConstraintRule assertConstraintRule( SchemaRule schemaRule )
    {
        if ( !(schemaRule instanceof ConstraintRule) )
        {
            fail( "Expected ConstraintRule, but got " + schemaRule.getClass().getSimpleName() );
        }
        return (ConstraintRule)schemaRule;
    }

    private void assertCorrectLength( StorageIndexReference indexRule )
    {
        // GIVEN
        ByteBuffer buffer = ByteBuffer.wrap( SchemaRuleSerialization35.serialize( indexRule ) );

        // THEN
        assertThat( SchemaRuleSerialization35.lengthOf( indexRule ), equalTo( buffer.capacity() ) );
    }

    private void assertCorrectLength( ConstraintRule constraintRule )
    {
        // GIVEN
        ByteBuffer buffer = ByteBuffer.wrap( SchemaRuleSerialization35.serialize( constraintRule ) );

        // THEN
        assertThat( SchemaRuleSerialization35.lengthOf( constraintRule ), equalTo( buffer.capacity() ) );
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

    private String addNullByte( int nullByteCountToAdd, String input )
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

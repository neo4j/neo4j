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
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.internal.schema.constraints.NodeKeyConstraintDescriptor;
import org.neo4j.internal.schema.constraints.UniquenessConstraintDescriptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.internal.schema.SchemaDescriptor.fulltext;
import static org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory.existsForLabel;
import static org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory.existsForRelType;
import static org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory.nodeKeyForLabel;
import static org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory.uniqueForLabel;

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
    private static final String PROVIDER_VERSION_25 = "25.0";
    private static final IndexProviderDescriptor PROVIDER = new IndexProviderDescriptor( PROVIDER_KEY, PROVIDER_VERSION );
    private static final IndexProviderDescriptor PROVIDER_25 = new IndexProviderDescriptor( PROVIDER_KEY, PROVIDER_VERSION_25 );

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
        if ( schema.isFulltextSchemaDescriptor() )
        {
            prototype = prototype.withIndexType( IndexType.FULLTEXT );
        }
        return prototype;
    }

    private static IndexPrototype forLabel( int labelId, int... propertyIds )
    {
        return indexPrototype( false, null, labelId, propertyIds );
    }

    private static IndexPrototype namedForLabel( String name, int labelId, int... propertyIds )
    {
        return indexPrototype( false, name, labelId, propertyIds );
    }

    private static IndexPrototype uniqueForLabelProto( int labelId, int... propertyIds )
    {
        return indexPrototype( true, null, labelId, propertyIds );
    }

    private static IndexPrototype namedUniqueForLabel( String name, int labelId, int... propertyIds )
    {
        return indexPrototype( true, name, labelId, propertyIds );
    }

    static IndexDescriptor withId( IndexPrototype prototype, long id )
    {
        if ( prototype.getName().isEmpty() )
        {
            prototype = prototype.withName( "index_" + id );
        }
        return prototype.materialise( id );
    }

    static ConstraintDescriptor withId( ConstraintDescriptor constraint, long id )
    {
        if ( constraint.getName() == null )
        {
            constraint = constraint.withName( "constraint_" + id );
        }
        return constraint.withId( id );
    }

    private static IndexDescriptor withIds( IndexPrototype prototype, long id, long owningConstraintId )
    {
        return withId( prototype, id ).withOwningConstraintId( owningConstraintId );
    }

    private final IndexDescriptor indexRegular = withId( forLabel( LABEL_ID, PROPERTY_ID_1 ), RULE_ID );

    private final IndexDescriptor indexUnique = withIds( uniqueForLabelProto( LABEL_ID, PROPERTY_ID_1 ), RULE_ID_2, RULE_ID );

    private final IndexDescriptor indexCompositeRegular = withId( forLabel( LABEL_ID, PROPERTY_ID_1, PROPERTY_ID_2 ), RULE_ID );

    private final IndexDescriptor indexMultiTokenRegular = withId(
            indexPrototype( false, null,
                    fulltext( EntityType.NODE, new int[]{LABEL_ID, LABEL_ID_2}, new int[]{PROPERTY_ID_1, PROPERTY_ID_2} ) ),
            RULE_ID );

    private final IndexDescriptor indexCompositeUnique = withIds( uniqueForLabelProto( LABEL_ID, PROPERTY_ID_1, PROPERTY_ID_2 ), RULE_ID_2, RULE_ID );

    private final IndexDescriptor indexBigComposite = withId( forLabel( LABEL_ID, IntStream.range(1, 200).toArray() ), RULE_ID );

    private final IndexDescriptor indexBigMultiToken = withId(
            indexPrototype( false, null,
                    fulltext( EntityType.RELATIONSHIP, IntStream.range( 1, 200 ).toArray(), IntStream.range( 1, 200 ).toArray() ) ),
            RULE_ID );

    private final ConstraintDescriptor constraintExistsLabel = withId( existsForLabel( LABEL_ID, PROPERTY_ID_1 ), RULE_ID );

    private final ConstraintDescriptor constraintUniqueLabel =
            withId( uniqueForLabel( LABEL_ID, PROPERTY_ID_1 ), RULE_ID_2 ).withOwnedIndexId( RULE_ID );

    private final ConstraintDescriptor constraintNodeKeyLabel =
            withId( nodeKeyForLabel( LABEL_ID, PROPERTY_ID_1 ), RULE_ID_2 ).withOwnedIndexId( RULE_ID );

    private final ConstraintDescriptor constraintExistsRelType =
            withId( existsForRelType( REL_TYPE_ID, PROPERTY_ID_1 ), RULE_ID_2 );

    private final ConstraintDescriptor constraintCompositeLabel =
            withId( existsForLabel( LABEL_ID, PROPERTY_ID_1, PROPERTY_ID_2 ), RULE_ID );

    private final ConstraintDescriptor constraintCompositeRelType =
            withId( existsForRelType( REL_TYPE_ID, PROPERTY_ID_1, PROPERTY_ID_2 ), RULE_ID_2 );

    // INDEX RULES

    @Test
    void rulesCreatedWithoutNameMustHaveComputedName()
    {
        assertThat( indexRegular.getName() ).isEqualTo( "index_1" );
        assertThat( indexUnique.getName() ).isEqualTo( "index_2" );
        assertThat( indexCompositeRegular.getName() ).isEqualTo( "index_1" );
        assertThat( indexCompositeUnique.getName() ).isEqualTo( "index_2" );
        assertThat( indexBigComposite.getName() ).isEqualTo( "index_1" );
        assertThat( constraintExistsLabel.getName() ).isEqualTo( "constraint_1" );
        assertThat( constraintUniqueLabel.getName() ).isEqualTo( "constraint_2" );
        assertThat( constraintNodeKeyLabel.getName() ).isEqualTo( "constraint_2" );
        assertThat( constraintExistsRelType.getName() ).isEqualTo( "constraint_2" );
        assertThat( constraintCompositeLabel.getName() ).isEqualTo( "constraint_1" );
        assertThat( constraintCompositeRelType.getName() ).isEqualTo( "constraint_2" );
    }

    @Test
    void rulesCreatedWithoutNameMustRetainComputedNameAfterDeserialisation() throws Exception
    {
        assertThat( serialiseAndDeserialise( indexRegular ).getName() ).isEqualTo( "index_1" );
        assertThat( serialiseAndDeserialise( indexUnique ).getName() ).isEqualTo( "index_2" );
        assertThat( serialiseAndDeserialise( indexCompositeRegular ).getName() ).isEqualTo( "index_1" );
        assertThat( serialiseAndDeserialise( indexCompositeUnique ).getName() ).isEqualTo( "index_2" );
        assertThat( serialiseAndDeserialise( indexBigComposite ).getName() ).isEqualTo( "index_1" );
        assertThat( serialiseAndDeserialise( constraintExistsLabel ).getName() ).isEqualTo( "constraint_1" );
        assertThat( serialiseAndDeserialise( constraintUniqueLabel ).getName() ).isEqualTo( "constraint_2" );
        assertThat( serialiseAndDeserialise( constraintNodeKeyLabel ).getName() ).isEqualTo( "constraint_2" );
        assertThat( serialiseAndDeserialise( constraintExistsRelType ).getName() ).isEqualTo( "constraint_2" );
        assertThat( serialiseAndDeserialise( constraintCompositeLabel ).getName() ).isEqualTo( "constraint_1" );
        assertThat( serialiseAndDeserialise( constraintCompositeRelType ).getName() ).isEqualTo( "constraint_2" );
    }

    @Test
    void rulesCreatedWithNameMustRetainGivenNameAfterDeserialisation() throws Exception
    {
        String name = "custom_rule";

        assertThat( serialiseAndDeserialise( withId( namedForLabel( name, LABEL_ID, PROPERTY_ID_1 ), RULE_ID ) ).getName() ).isEqualTo( name );
        assertThat( serialiseAndDeserialise( withIds( namedUniqueForLabel( name, LABEL_ID, PROPERTY_ID_1 ), RULE_ID_2, RULE_ID ) ).getName() ).isEqualTo(
                name );
        assertThat( serialiseAndDeserialise( withId( namedForLabel( name, LABEL_ID, PROPERTY_ID_1, PROPERTY_ID_2 ), RULE_ID ) ).getName() ).isEqualTo( name );
        assertThat( serialiseAndDeserialise(
                withIds( namedUniqueForLabel( name, LABEL_ID, PROPERTY_ID_1, PROPERTY_ID_2 ), RULE_ID_2, RULE_ID ) ).getName() ).isEqualTo( name );
        assertThat( serialiseAndDeserialise( withId( namedForLabel( name, LABEL_ID, IntStream.range( 1, 200 ).toArray() ), RULE_ID ) ).getName() ).isEqualTo(
                name );
        assertThat( serialiseAndDeserialise( existsForLabel( LABEL_ID, PROPERTY_ID_1 ).withId( RULE_ID ).withName( name ) ).getName() ).isEqualTo( name );
        assertThat( serialiseAndDeserialise(
                uniqueForLabel( LABEL_ID, PROPERTY_ID_1 ).withId( RULE_ID_2 ).withOwnedIndexId( RULE_ID ).withName( name ) ).getName() ).isEqualTo( name );
        assertThat( serialiseAndDeserialise(
                nodeKeyForLabel( LABEL_ID, PROPERTY_ID_1 ).withId( RULE_ID_2 ).withOwnedIndexId( RULE_ID ).withName( name ) ).getName() ).isEqualTo( name );
        assertThat( serialiseAndDeserialise( existsForRelType( REL_TYPE_ID, PROPERTY_ID_1 ).withId( RULE_ID_2 ).withName( name ) ).getName() ).isEqualTo(
                name );
        assertThat(
                serialiseAndDeserialise( existsForLabel( LABEL_ID, PROPERTY_ID_1, PROPERTY_ID_2 ).withId( RULE_ID ).withName( name ) ).getName() ).isEqualTo(
                name );
        assertThat( serialiseAndDeserialise(
                existsForRelType( REL_TYPE_ID, PROPERTY_ID_1, PROPERTY_ID_2 ).withId( RULE_ID_2 ).withName( name ) ).getName() ).isEqualTo( name );
    }

    @Test
    void rulesCreatedWithNullNameMustRetainComputedNameAfterDeserialisation() throws Exception
    {
        assertThat( serialiseAndDeserialise( withId( forLabel( LABEL_ID, PROPERTY_ID_1 ), RULE_ID ) ).getName() ).isEqualTo( "index_1" );
        assertThat( serialiseAndDeserialise( withIds( uniqueForLabelProto( LABEL_ID, PROPERTY_ID_1 ), RULE_ID_2, RULE_ID ) ).getName() ).isEqualTo( "index_2" );
        assertThat( serialiseAndDeserialise( withId( forLabel( LABEL_ID, PROPERTY_ID_1, PROPERTY_ID_2 ), RULE_ID ) ).getName() ).isEqualTo( "index_1" );
        assertThat(
                serialiseAndDeserialise( withIds( uniqueForLabelProto( LABEL_ID, PROPERTY_ID_1, PROPERTY_ID_2 ), RULE_ID_2, RULE_ID ) ).getName() ).isEqualTo(
                "index_2" );
        assertThat( serialiseAndDeserialise( withId( forLabel( LABEL_ID, IntStream.range( 1, 200 ).toArray() ), RULE_ID ) ).getName() ).isEqualTo( "index_1" );

        assertThat( serialiseAndDeserialise( withId( existsForLabel( LABEL_ID, PROPERTY_ID_1 ), RULE_ID ) ).getName() ).isEqualTo( "constraint_1" );
        assertThat( serialiseAndDeserialise( withId( uniqueForLabel( LABEL_ID, PROPERTY_ID_1 ), RULE_ID_2 ).withOwnedIndexId( RULE_ID ) ).getName() ).isEqualTo(
                "constraint_2" );
        assertThat(
                serialiseAndDeserialise( withId( nodeKeyForLabel( LABEL_ID, PROPERTY_ID_1 ), RULE_ID_2 ).withOwnedIndexId( RULE_ID ) ).getName() ).isEqualTo(
                "constraint_2" );
        assertThat( serialiseAndDeserialise( withId( existsForRelType( REL_TYPE_ID, PROPERTY_ID_1 ), RULE_ID_2 ) ).getName() ).isEqualTo( "constraint_2" );
        assertThat( serialiseAndDeserialise( withId( existsForLabel( LABEL_ID, PROPERTY_ID_1, PROPERTY_ID_2 ), RULE_ID ) ).getName() ).isEqualTo(
                "constraint_1" );
        assertThat( serialiseAndDeserialise( withId( existsForRelType( REL_TYPE_ID, PROPERTY_ID_1, PROPERTY_ID_2 ), RULE_ID_2 ) ).getName() ).isEqualTo(
                "constraint_2" );
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
        assertThrows( IllegalArgumentException.class, () -> withId( namedForLabel( "", LABEL_ID, PROPERTY_ID_1 ), RULE_ID ) );
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
        String name =  "";
        assertThrows( IllegalArgumentException.class, () -> withIds( namedUniqueForLabel( name, LABEL_ID, PROPERTY_ID_1 ), RULE_ID, RULE_ID_2 ) );
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
        assertParseUniqueConstraintRule( "/////ww+AAAAAAAAAAJbAAAANwABAAAAAw==", null );
        assertParseUniqueConstraintRule( "AAAANwMBAAAAAAAAAAMAAAAAAAAAAg==", null ); // LEGACY
        assertParseUniqueConstraintRule( "/////ww+AAAAAAAAAAJbAAAANwABAAAAAwAAAAtjdXN0b21fbmFtZQ==",
                "custom_name" ); // named rule
        assertParseUniqueConstraintRule( addNullByte( "/////ww+AAAAAAAAAAJbAAAANwABAAAAAw==" ),
                null ); // empty name
        assertParseUniqueConstraintRule( addNullByte( 2, "/////ww+AAAAAAAAAAJbAAAANwABAAAAAw==" ),
                null ); // empty name
        assertParseUniqueConstraintRule( addNullByte( 3, "/////ww+AAAAAAAAAAJbAAAANwABAAAAAw==" ),
                null ); // empty name
        assertParseUniqueConstraintRule( addNullByte( 4, "/////ww+AAAAAAAAAAJbAAAANwABAAAAAw==" ),
                null ); // empty name
        assertParseUniqueConstraintRule( addNullByte( 5, "/////ww+AAAAAAAAAAJbAAAANwABAAAAAw==" ),
                null ); // empty name
    }

    @Test
    void shouldParseNodeKeyConstraintRule() throws Exception
    {
        assertParseNodeKeyConstraintRule( "/////ww/AAAAAAAAAAJbAAAANwABAAAAAw==", null );
        assertParseNodeKeyConstraintRule( "/////ww/AAAAAAAAAAJbAAAANwABAAAAAwAAAAtjdXN0b21fbmFtZQ==",
                "custom_name" ); // named rule
        assertParseNodeKeyConstraintRule( addNullByte( "/////ww/AAAAAAAAAAJbAAAANwABAAAAAw==" ),
                null ); // empty name
        assertParseNodeKeyConstraintRule( addNullByte( 2, "/////ww/AAAAAAAAAAJbAAAANwABAAAAAw==" ),
                null ); // empty name
        assertParseNodeKeyConstraintRule( addNullByte( 3, "/////ww/AAAAAAAAAAJbAAAANwABAAAAAw==" ),
                null ); // empty name
        assertParseNodeKeyConstraintRule( addNullByte( 4, "/////ww/AAAAAAAAAAJbAAAANwABAAAAAw==" ),
                null ); // empty name
        assertParseNodeKeyConstraintRule( addNullByte( 5, "/////ww/AAAAAAAAAAJbAAAANwABAAAAAw==" ),
                null ); // empty name
    }

    @Test
    void shouldParseNodePropertyExistsRule() throws Exception
    {
        assertParseNodePropertyExistsRule( "/////ww9WwAAAC0AAQAAADM=", null );
        assertParseNodePropertyExistsRule( "AAAALQQAAAAz", null ); // LEGACY
        assertParseNodePropertyExistsRule( "/////ww9WwAAAC0AAQAAADMAAAALY3VzdG9tX25hbWU=",
                "custom_name" ); // named rule
        assertParseNodePropertyExistsRule( addNullByte( "/////ww9WwAAAC0AAQAAADM=" ), null ); // empty name
        assertParseNodePropertyExistsRule( addNullByte( 2, "/////ww9WwAAAC0AAQAAADM=" ),
                null ); // empty name
        assertParseNodePropertyExistsRule( addNullByte( 3, "/////ww9WwAAAC0AAQAAADM=" ),
                null ); // empty name
        assertParseNodePropertyExistsRule( addNullByte( 4, "/////ww9WwAAAC0AAQAAADM=" ),
                null ); // empty name
        assertParseNodePropertyExistsRule( addNullByte( 5, "/////ww9WwAAAC0AAQAAADM=" ),
                null ); // empty name
    }

    @Test
    void shouldParseRelationshipPropertyExistsRule() throws Exception
    {
        assertParseRelationshipPropertyExistsRule( "/////ww9XAAAIUAAAQAAF+c=", null );
        assertParseRelationshipPropertyExistsRule( "AAAhQAUAABfn", null ); // LEGACY6
        assertParseRelationshipPropertyExistsRule( "/////ww9XAAAIUAAAQAAF+cAAAALY3VzdG9tX25hbWU=",
                "custom_name" ); // named rule
        assertParseRelationshipPropertyExistsRule( addNullByte( "/////ww9XAAAIUAAAQAAF+c=" ),
                null ); // empty name
        assertParseRelationshipPropertyExistsRule( addNullByte( 2, "/////ww9XAAAIUAAAQAAF+c=" ),
                null ); // empty name
        assertParseRelationshipPropertyExistsRule( addNullByte( 3, "/////ww9XAAAIUAAAQAAF+c=" ),
                null ); // empty name
        assertParseRelationshipPropertyExistsRule( addNullByte( 4, "/////ww9XAAAIUAAAQAAF+c=" ),
                null ); // empty name
        assertParseRelationshipPropertyExistsRule( addNullByte( 5, "/////ww9XAAAIUAAAQAAF+c=" ),
                null ); // empty name
    }

    private static void assertParseIndexRule( String serialized, String name ) throws Exception
    {
        // GIVEN
        long ruleId = 24;
        IndexPrototype prototype = forLabel( 512, 4 ).withIndexProvider( PROVIDER_25 );
        byte[] bytes = decodeBase64( serialized );

        // WHEN
        IndexDescriptor deserialized = assertIndexRule( SchemaRuleSerialization35.deserialize( ruleId, ByteBuffer.wrap( bytes ) ) );

        // THEN
        assertThat( deserialized.getId() ).isEqualTo( ruleId );
        assertThat( deserialized ).isEqualTo( prototype.withName( name ).materialise( ruleId ) );
        assertThat( deserialized.schema() ).isEqualTo( prototype.schema() );
        assertThat( deserialized.getIndexProvider().getKey() ).isEqualTo( PROVIDER_KEY );
        assertThat( deserialized.getIndexProvider().getVersion() ).isEqualTo( PROVIDER_VERSION_25 );
        assertThat( deserialized.getName() ).isEqualTo( name );
        assertTrue( deserialized.getOwningConstraintId().isEmpty() );
    }

    private static void assertParseUniqueIndexRule( String serialized, String name ) throws MalformedSchemaRuleException
    {
        // GIVEN
        long ruleId = 33;
        long constraintId = 11;
        IndexPrototype prototype = uniqueForLabelProto( 61, 988 ).withIndexProvider( PROVIDER_25 );
        byte[] bytes = decodeBase64( serialized );

        // WHEN
        IndexDescriptor deserialized = assertIndexRule( SchemaRuleSerialization35.deserialize( ruleId, ByteBuffer.wrap( bytes ) ) );

        // THEN
        assertThat( deserialized.getId() ).isEqualTo( ruleId );
        assertThat( deserialized ).isEqualTo( prototype.withName( name ).materialise( ruleId ) );
        assertThat( deserialized.schema() ).isEqualTo( prototype.schema() );
        assertThat( deserialized.getIndexProvider().getKey() ).isEqualTo( PROVIDER_KEY );
        assertThat( deserialized.getIndexProvider().getVersion() ).isEqualTo( PROVIDER_VERSION_25 );
        assertThat( deserialized.getOwningConstraintId().isPresent() ).isEqualTo( true );
        assertThat( deserialized.getOwningConstraintId().getAsLong() ).isEqualTo( constraintId );
        assertThat( deserialized.getName() ).isEqualTo( name );
    }

    private static void assertParseUniqueConstraintRule( String serialized, String name ) throws MalformedSchemaRuleException
    {
        // GIVEN
        long ruleId = 1;
        int propertyKey = 3;
        int labelId = 55;
        long ownedIndexId = 2;
        UniquenessConstraintDescriptor constraint = uniqueForLabel( labelId, propertyKey );
        byte[] bytes = decodeBase64( serialized );

        // WHEN
        ConstraintDescriptor deserialized = assertConstraintRule( SchemaRuleSerialization35.deserialize( ruleId, ByteBuffer.wrap( bytes ) ) );

        // THEN
        assertThat( deserialized.getId() ).isEqualTo( ruleId );
        assertThat( deserialized ).isEqualTo( constraint );
        assertThat( deserialized.schema() ).isEqualTo( constraint.schema() );
        assertThat( deserialized.asIndexBackedConstraint().ownedIndexId() ).isEqualTo( ownedIndexId );
        assertThat( deserialized.getName() ).isEqualTo( name );
    }

    private static void assertParseNodeKeyConstraintRule( String serialized, String name ) throws MalformedSchemaRuleException
    {
        // GIVEN
        long ruleId = 1;
        int propertyKey = 3;
        int labelId = 55;
        long ownedIndexId = 2;
        NodeKeyConstraintDescriptor constraint = nodeKeyForLabel( labelId, propertyKey );
        byte[] bytes = decodeBase64( serialized );

        // WHEN
        ConstraintDescriptor deserialized = assertConstraintRule( SchemaRuleSerialization35.deserialize( ruleId, ByteBuffer.wrap( bytes ) ) );

        // THEN
        assertThat( deserialized.getId() ).isEqualTo( ruleId );
        assertThat( deserialized ).isEqualTo( constraint );
        assertThat( deserialized.schema() ).isEqualTo( constraint.schema() );
        assertThat( deserialized.asIndexBackedConstraint().ownedIndexId() ).isEqualTo( ownedIndexId );
        assertThat( deserialized.getName() ).isEqualTo( name );
    }

    private static void assertParseNodePropertyExistsRule( String serialized, String name ) throws Exception
    {
        // GIVEN
        long ruleId = 87;
        int propertyKey = 51;
        int labelId = 45;
        ConstraintDescriptor constraint = existsForLabel( labelId, propertyKey );
        byte[] bytes = decodeBase64( serialized );

        // WHEN
        ConstraintDescriptor deserialized = assertConstraintRule( SchemaRuleSerialization35.deserialize( ruleId, ByteBuffer.wrap( bytes ) ) );

        // THEN
        assertThat( deserialized.getId() ).isEqualTo( ruleId );
        assertThat( deserialized ).isEqualTo( constraint );
        assertThat( deserialized.schema() ).isEqualTo( constraint.schema() );
        assertThatThrownBy( () -> deserialized.asIndexBackedConstraint().ownedIndexId() ).isInstanceOf( IllegalStateException.class );
        assertThat( deserialized.getName() ).isEqualTo( name );
    }

    private static void assertParseRelationshipPropertyExistsRule( String serialized, String name ) throws Exception
    {
        // GIVEN
        long ruleId = 51;
        int propertyKey = 6119;
        int relTypeId = 8512;
        ConstraintDescriptor constraint = existsForRelType( relTypeId, propertyKey );
        byte[] bytes = decodeBase64( serialized );

        // WHEN
        ConstraintDescriptor deserialized = assertConstraintRule( SchemaRuleSerialization35.deserialize( ruleId, ByteBuffer.wrap( bytes ) ) );

        // THEN
        assertThat( deserialized.getId() ).isEqualTo( ruleId );
        assertThat( deserialized ).isEqualTo( constraint );
        assertThat( deserialized.schema() ).isEqualTo( constraint.schema() );
        assertThatThrownBy( () -> deserialized.asIndexBackedConstraint().ownedIndexId() ).isInstanceOf( IllegalStateException.class );
        assertThat( deserialized.getName() ).isEqualTo( name );
    }

    // HELPERS

    private static void assertSerializeAndDeserializeIndexRule( IndexDescriptor indexRule )
            throws MalformedSchemaRuleException
    {
        IndexDescriptor deserialized = assertIndexRule( serialiseAndDeserialise( indexRule ) );

        assertThat( deserialized.getId() ).isEqualTo( indexRule.getId() );
        assertThat( deserialized ).isEqualTo( indexRule );
        assertThat( deserialized.schema() ).isEqualTo( indexRule.schema() );
        assertThat( deserialized.getIndexProvider() ).isEqualTo( indexRule.getIndexProvider() );
    }

    private static IndexDescriptor assertIndexRule( SchemaRule schemaRule )
    {
        if ( !(schemaRule instanceof IndexDescriptor) )
        {
            fail( "Expected IndexRule, but got " + schemaRule.getClass().getSimpleName() );
        }
        return (IndexDescriptor)schemaRule;
    }

    private static void assertSerializeAndDeserializeConstraintRule( ConstraintDescriptor constraintRule )
            throws MalformedSchemaRuleException
    {
        ConstraintDescriptor deserialized = assertConstraintRule( serialiseAndDeserialise( constraintRule ) );

        assertThat( deserialized.getId() ).isEqualTo( constraintRule.getId() );
        assertThat( deserialized ).isEqualTo( constraintRule );
        assertThat( deserialized.schema() ).isEqualTo( constraintRule.schema() );
    }

    private static SchemaRule serialiseAndDeserialise( ConstraintDescriptor constraintRule ) throws MalformedSchemaRuleException
    {
        ByteBuffer buffer = ByteBuffer.wrap( SchemaRuleSerialization35.serialize( constraintRule ) );
        return SchemaRuleSerialization35.deserialize( constraintRule.getId(), buffer );
    }

    private static SchemaRule serialiseAndDeserialise( IndexDescriptor indexRule ) throws MalformedSchemaRuleException
    {
        ByteBuffer buffer = ByteBuffer.wrap( SchemaRuleSerialization35.serialize( indexRule ) );
        return SchemaRuleSerialization35.deserialize( indexRule.getId(), buffer );
    }

    private static ConstraintDescriptor assertConstraintRule( SchemaRule schemaRule )
    {
        if ( !(schemaRule instanceof ConstraintDescriptor) )
        {
            fail( "Expected ConstraintDescriptor, but got " + schemaRule.getClass().getSimpleName() );
        }
        return (ConstraintDescriptor)schemaRule;
    }

    private static void assertCorrectLength( IndexDescriptor indexRule )
    {
        // GIVEN
        ByteBuffer buffer = ByteBuffer.wrap( SchemaRuleSerialization35.serialize( indexRule ) );

        // THEN
        assertThat( SchemaRuleSerialization35.lengthOf( indexRule ) ).isEqualTo( buffer.capacity() );
    }

    private static void assertCorrectLength( ConstraintDescriptor constraintRule )
    {
        // GIVEN
        ByteBuffer buffer = ByteBuffer.wrap( SchemaRuleSerialization35.serialize( constraintRule ) );

        // THEN
        assertThat( SchemaRuleSerialization35.lengthOf( constraintRule ) ).isEqualTo( buffer.capacity() );
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

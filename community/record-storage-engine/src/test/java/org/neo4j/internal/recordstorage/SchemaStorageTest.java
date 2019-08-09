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
package org.neo4j.internal.recordstorage;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.stream.Stream;

import org.neo4j.common.EntityType;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.configuration.Config;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.kernel.api.exceptions.schema.DuplicateSchemaRuleException;
import org.neo4j.internal.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.EphemeralPageCacheExtension;
import org.neo4j.test.mockito.matcher.KernelExceptionUserMessageMatcher;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;

@SuppressWarnings( "unchecked" )
@EphemeralPageCacheExtension
class SchemaStorageTest
{
    private static final String LABEL1 = "Label1";
    private static final int LABEL1_ID = 1;
    private static final String TYPE1 = "Type1";
    private static final int TYPE1_ID = 1;
    private static final String PROP1 = "prop1";
    private static final int PROP1_ID = 1;

    @Inject
    private PageCache pageCache;
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private EphemeralFileSystemAbstraction fs;

    private SchemaStorage storage;
    private NeoStores neoStores;

    @BeforeEach
    void before()
    {
        var storeFactory = new StoreFactory( testDirectory.databaseLayout(), Config.defaults(), new DefaultIdGeneratorFactory( fs, immediate() ),
            pageCache, fs, NullLogProvider.getInstance() );
        neoStores = storeFactory.openNeoStores( true, StoreType.SCHEMA, StoreType.PROPERTY_KEY_TOKEN, StoreType.LABEL_TOKEN,
            StoreType.RELATIONSHIP_TYPE_TOKEN );
        storage = new SchemaStorage( neoStores.getSchemaStore(), StoreTokens.readOnlyTokenHolders( neoStores ) );
    }

    @AfterEach
    void after()
    {
        neoStores.close();
    }

    @Test
    void shouldThrowExceptionOnNodeRuleNotFound()
    {
        TokenNameLookup tokenNameLookup = getDefaultTokenNameLookup();

        var e = assertThrows( SchemaRuleNotFoundException.class, () ->
            storage.constraintsGetSingle( ConstraintDescriptorFactory.existsForLabel( LABEL1_ID, PROP1_ID ) ) );

        assertThat( e, new KernelExceptionUserMessageMatcher( tokenNameLookup,
            "No label property existence constraint was found for :Label1(prop1)." ) );
    }

    @Test
    void shouldThrowExceptionOnNodeDuplicateRuleFound()
    {
        TokenNameLookup tokenNameLookup = getDefaultTokenNameLookup();

        SchemaStorage schemaStorageSpy = Mockito.spy( storage );
        when( schemaStorageSpy.streamAllSchemaRules( false ) ).thenReturn(
            Stream.of(
                getUniquePropertyConstraintRule( 1L, LABEL1_ID, PROP1_ID ),
                getUniquePropertyConstraintRule( 2L, LABEL1_ID, PROP1_ID ) ) );

        var e = assertThrows( DuplicateSchemaRuleException.class, () ->
            schemaStorageSpy.constraintsGetSingle( ConstraintDescriptorFactory.uniqueForLabel( LABEL1_ID, PROP1_ID ) ) );

        assertThat( e, new KernelExceptionUserMessageMatcher( tokenNameLookup,
            "Multiple label uniqueness constraints found for :Label1(prop1)." ) );
    }

    @Test
    void shouldThrowExceptionOnRelationshipRuleNotFound()
    {
        TokenNameLookup tokenNameLookup = getDefaultTokenNameLookup();

        var e = assertThrows( SchemaRuleNotFoundException.class, () ->
            storage.constraintsGetSingle( ConstraintDescriptorFactory.existsForRelType( TYPE1_ID, PROP1_ID ) ) );
        assertThat( e,
            new KernelExceptionUserMessageMatcher( tokenNameLookup,
                "No relationship type property existence constraint was found for -[:Type1(prop1)]-." ) );
    }

    @Test
    void shouldThrowExceptionOnRelationshipDuplicateRuleFound()
    {
        TokenNameLookup tokenNameLookup = getDefaultTokenNameLookup();

        SchemaStorage schemaStorageSpy = Mockito.spy( storage );
        when( schemaStorageSpy.streamAllSchemaRules( false ) ).thenReturn(
            Stream.of(
                getRelationshipPropertyExistenceConstraintRule( 1L, TYPE1_ID, PROP1_ID ),
                getRelationshipPropertyExistenceConstraintRule( 2L, TYPE1_ID, PROP1_ID ) ) );

        var e = assertThrows( DuplicateSchemaRuleException.class, () ->
            schemaStorageSpy.constraintsGetSingle( ConstraintDescriptorFactory.existsForRelType( TYPE1_ID, PROP1_ID ) ) );

        assertThat( e,
            new KernelExceptionUserMessageMatcher( tokenNameLookup,
                "Multiple relationship type property existence constraints found for -[:Type1(prop1)]-." ) );
    }

    private static TokenNameLookup getDefaultTokenNameLookup()
    {
        TokenNameLookup tokenNameLookup = mock( TokenNameLookup.class );
        when( tokenNameLookup.labelGetName( LABEL1_ID ) ).thenReturn( LABEL1 );
        when( tokenNameLookup.propertyKeyGetName( PROP1_ID ) ).thenReturn( PROP1 );
        when( tokenNameLookup.relationshipTypeGetName( TYPE1_ID ) ).thenReturn( TYPE1 );
        when( tokenNameLookup.entityTokensGetNames( EntityType.NODE, new int[]{LABEL1_ID} ) ).thenReturn( new String[]{LABEL1} );
        when( tokenNameLookup.entityTokensGetNames( EntityType.RELATIONSHIP, new int[]{TYPE1_ID} ) ).thenReturn( new String[]{TYPE1} );
        return tokenNameLookup;
    }

    private static ConstraintDescriptor getUniquePropertyConstraintRule( long id, int label, int property )
    {
        return ConstraintDescriptorFactory.uniqueForLabel( label, property ).withId( id ).withOwnedIndexId( 0 );
    }

    private static ConstraintDescriptor getRelationshipPropertyExistenceConstraintRule( long id, int type, int property )
    {
        return ConstraintDescriptorFactory.existsForRelType( type, property ).withId( id );
    }
}

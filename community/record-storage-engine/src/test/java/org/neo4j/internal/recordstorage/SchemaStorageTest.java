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

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.stream.Stream;

import org.neo4j.common.TokenNameLookup;
import org.neo4j.internal.kernel.api.exceptions.schema.DuplicateSchemaRuleException;
import org.neo4j.internal.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.SchemaStore;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.storageengine.api.ConstraintRule;
import org.neo4j.test.mockito.matcher.KernelExceptionUserMessageMatcher;
import org.neo4j.test.rule.NeoStoresRule;
import org.neo4j.test.rule.PageCacheAndDependenciesRule;
import org.neo4j.token.TokenHolders;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SchemaStorageTest
{
    private static final String LABEL1 = "Label1";
    private static final int LABEL1_ID = 1;
    private static final String TYPE1 = "Type1";
    private static final int TYPE1_ID = 1;
    private static final String PROP1 = "prop1";
    private static final int PROP1_ID = 1;

    @Rule
    public PageCacheAndDependenciesRule storageRule = new PageCacheAndDependenciesRule();

    @Rule
    public NeoStoresRule storesRule = new NeoStoresRule( SchemaStorageTest.class,
            StoreType.SCHEMA, StoreType.PROPERTY_KEY_TOKEN, StoreType.LABEL_TOKEN, StoreType.RELATIONSHIP_TYPE_TOKEN );

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private SchemaStorage storage;
    private NeoStores neoStores;

    @Before
    public void setup() throws IOException
    {
        neoStores = storesRule.builder().with( storageRule.fileSystem() ).with( storageRule.pageCache() ).build();
        SchemaStore store = neoStores.getSchemaStore();
        storage = new SchemaStorage( store, StoreTokens.readOnlyTokenHolders( neoStores ) );
    }

    @After
    public void close()
    {
        neoStores.close();
    }

    @Test
    public void shouldThrowExceptionOnNodeRuleNotFound()
            throws DuplicateSchemaRuleException, SchemaRuleNotFoundException
    {
        // GIVEN
        TokenNameLookup tokenNameLookup = getDefaultTokenNameLookup();

        // EXPECT
        expectedException.expect( SchemaRuleNotFoundException.class );
        expectedException.expect( new KernelExceptionUserMessageMatcher( tokenNameLookup,
                "No node property existence constraint was found for :Label1(prop1)." ) );

        // WHEN
        storage.constraintsGetSingle( ConstraintDescriptorFactory.existsForLabel( LABEL1_ID, PROP1_ID ) );
    }

    @Test
    public void shouldThrowExceptionOnNodeDuplicateRuleFound()
            throws DuplicateSchemaRuleException, SchemaRuleNotFoundException
    {
        // GIVEN
        TokenNameLookup tokenNameLookup = getDefaultTokenNameLookup();

        SchemaStorage schemaStorageSpy = Mockito.spy( storage );
        Mockito.when( schemaStorageSpy.streamAllSchemaRules( false ) ).thenReturn(
                Stream.of(
                        getUniquePropertyConstraintRule( 1L, LABEL1_ID, PROP1_ID ),
                        getUniquePropertyConstraintRule( 2L, LABEL1_ID, PROP1_ID ) ) );

        //EXPECT
        expectedException.expect( DuplicateSchemaRuleException.class );
        expectedException.expect( new KernelExceptionUserMessageMatcher( tokenNameLookup,
                "Multiple uniqueness constraints found for :Label1(prop1)." ) );

        // WHEN
        schemaStorageSpy.constraintsGetSingle(
                ConstraintDescriptorFactory.uniqueForLabel( LABEL1_ID, PROP1_ID ) );
    }

    @Test
    public void shouldThrowExceptionOnRelationshipRuleNotFound()
            throws DuplicateSchemaRuleException, SchemaRuleNotFoundException
    {
        TokenNameLookup tokenNameLookup = getDefaultTokenNameLookup();

        // EXPECT
        expectedException.expect( SchemaRuleNotFoundException.class );
        expectedException.expect( new KernelExceptionUserMessageMatcher<>( tokenNameLookup,
                "No relationship property existence constraint was found for -[:Type1(prop1)]-." ) );

        //WHEN
        storage.constraintsGetSingle(
                ConstraintDescriptorFactory.existsForRelType( TYPE1_ID, PROP1_ID ) );
    }

    @Test
    public void shouldThrowExceptionOnRelationshipDuplicateRuleFound()
            throws DuplicateSchemaRuleException, SchemaRuleNotFoundException
    {
        // GIVEN
        TokenNameLookup tokenNameLookup = getDefaultTokenNameLookup();

        SchemaStorage schemaStorageSpy = Mockito.spy( storage );
        when( schemaStorageSpy.streamAllSchemaRules( false ) ).thenReturn(
                Stream.of(
                        getRelationshipPropertyExistenceConstraintRule( 1L, TYPE1_ID, PROP1_ID ),
                        getRelationshipPropertyExistenceConstraintRule( 2L, TYPE1_ID, PROP1_ID ) ) );

        //EXPECT
        expectedException.expect( DuplicateSchemaRuleException.class );
        expectedException.expect( new KernelExceptionUserMessageMatcher( tokenNameLookup,
                "Multiple relationship property existence constraints found for -[:Type1(prop1)]-." ) );

        // WHEN
        schemaStorageSpy.constraintsGetSingle(
                ConstraintDescriptorFactory.existsForRelType( TYPE1_ID, PROP1_ID ) );
    }

    private TokenNameLookup getDefaultTokenNameLookup()
    {
        TokenNameLookup tokenNameLookup = mock( TokenNameLookup.class );
        when( tokenNameLookup.labelGetName( LABEL1_ID ) ).thenReturn( LABEL1 );
        when( tokenNameLookup.propertyKeyGetName( PROP1_ID ) ).thenReturn( PROP1 );
        when( tokenNameLookup.relationshipTypeGetName( TYPE1_ID ) ).thenReturn( TYPE1 );
        return tokenNameLookup;
    }

    private ConstraintRule getUniquePropertyConstraintRule( long id, int label, int property )
    {
        return ConstraintRule.constraintRule( id, ConstraintDescriptorFactory.uniqueForLabel( label, property ), 0L );
    }

    private ConstraintRule getRelationshipPropertyExistenceConstraintRule( long id, int type, int property )
    {
        return ConstraintRule.constraintRule( id, ConstraintDescriptorFactory.existsForRelType( type, property ) );
    }
}

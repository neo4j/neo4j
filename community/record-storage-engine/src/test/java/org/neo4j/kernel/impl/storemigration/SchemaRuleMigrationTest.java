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
package org.neo4j.kernel.impl.storemigration;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.recordstorage.StoreTokens;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory;
import org.neo4j.kernel.impl.storemigration.legacy.SchemaStorage35;
import org.neo4j.storageengine.migration.SchemaRuleMigrationAccess;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.NamedToken;
import org.neo4j.token.api.TokenHolder;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SchemaRuleMigrationTest
{
    private TokenHolders srcTokenHolders;
    private SchemaStorage35 src;
    private List<SchemaRule> writtenRules;
    private SchemaRuleMigrationAccess dst;

    @BeforeEach
    void setUp()
    {
        srcTokenHolders = new TokenHolders(
                StoreTokens.createReadOnlyTokenHolder( TokenHolder.TYPE_PROPERTY_KEY ),
                StoreTokens.createReadOnlyTokenHolder( TokenHolder.TYPE_LABEL ),
                StoreTokens.createReadOnlyTokenHolder( TokenHolder.TYPE_RELATIONSHIP_TYPE ) );
        src = mock( SchemaStorage35.class );
        writtenRules = new ArrayList<>();
        dst = new SchemaRuleMigrationAccess()
        {
            @Override
            public void writeSchemaRule( SchemaRule rule )
            {
                writtenRules.add( rule );
            }

            @Override
            public Iterable<SchemaRule> getAll()
            {
                return List.of();
            }

            @Override
            public void close()
            {
            }
        };
    }

    @Test
    void mustEnsureThatMigratedSchemaRuleNamesAreUnique() throws KernelException
    {
        SchemaRule rule1 = IndexPrototype.forSchema( SchemaDescriptor.forLabel( 1, 2 ) ).withName( "a" ).materialise( 1 );
        SchemaRule rule2 = IndexPrototype.forSchema( SchemaDescriptor.forLabel( 1, 3 ) ).withName( "a" ).materialise( 2 );
        srcTokenHolders.labelTokens().setInitialTokens( List.of( new NamedToken( "Label", 1 ) ) );
        srcTokenHolders.propertyKeyTokens().setInitialTokens( List.of( new NamedToken( "a", 2 ), new NamedToken( "b", 3 ) ) );
        when( src.getAll() ).thenReturn( List.of( rule1, rule2 ) );

        RecordStorageMigrator.migrateSchemaRules( srcTokenHolders, src, dst );

        long distinctNames = writtenRules.stream().map( SchemaRule::getName ).distinct().count();
        assertEquals( 2, distinctNames );
    }

    @Test
    void constraintsWithoutNamesMustBeGivenGeneratedOnes() throws KernelException
    {
        SchemaRule rule = ConstraintDescriptorFactory.uniqueForSchema( SchemaDescriptor.forLabel( 1, 2 ) ).withId( 1 );
        srcTokenHolders.labelTokens().setInitialTokens( List.of( new NamedToken( "Label", 1 ) ) );
        srcTokenHolders.propertyKeyTokens().setInitialTokens( List.of( new NamedToken( "prop", 2 ) ) );
        when( src.getAll() ).thenReturn( List.of( rule ) );

        RecordStorageMigrator.migrateSchemaRules( srcTokenHolders, src, dst );

        assertEquals( 1, writtenRules.size() );
        assertEquals( "constraint_952591e6", writtenRules.get( 0 ).getName() );
    }

    @Test
    void mustOverwritePreviousDefaultIndexNames() throws KernelException
    {
        SchemaRule rule = IndexPrototype.forSchema( SchemaDescriptor.forLabel( 1, 2 ) ).withName( "index_1" ).materialise( 1 );
        srcTokenHolders.labelTokens().setInitialTokens( List.of( new NamedToken( "Label", 1 ) ) );
        srcTokenHolders.propertyKeyTokens().setInitialTokens( List.of( new NamedToken( "prop", 2 ) ) );
        when( src.getAll() ).thenReturn( List.of( rule ) );

        RecordStorageMigrator.migrateSchemaRules( srcTokenHolders, src, dst );

        assertEquals( 1, writtenRules.size() );
        assertEquals( "index_c3fbd584", writtenRules.get( 0 ).getName() );
    }

    @Test
    void mustOverwritePreviousDefaultConstraintNames() throws KernelException
    {
        SchemaRule rule = ConstraintDescriptorFactory.uniqueForSchema( SchemaDescriptor.forLabel( 1, 2 ) ).withId( 1 ).withName( "constraint_1" );
        srcTokenHolders.labelTokens().setInitialTokens( List.of( new NamedToken( "Label", 1 ) ) );
        srcTokenHolders.propertyKeyTokens().setInitialTokens( List.of( new NamedToken( "prop", 2 ) ) );
        when( src.getAll() ).thenReturn( List.of( rule ) );

        RecordStorageMigrator.migrateSchemaRules( srcTokenHolders, src, dst );

        assertEquals( 1, writtenRules.size() );
        assertEquals( "constraint_952591e6", writtenRules.get( 0 ).getName() );
    }

    @Test
    void mustEnsureUniqueNamesEvenWhenOldNamesMatchesNewDefaults() throws KernelException
    {
        SchemaRule rule1 = ConstraintDescriptorFactory.uniqueForSchema( SchemaDescriptor.forLabel( 1, 2 ) ).withId( 1 );
        SchemaRule rule2 = ConstraintDescriptorFactory.uniqueForSchema( SchemaDescriptor.forLabel( 1, 2 ) ).withId( 2 );
        srcTokenHolders.labelTokens().setInitialTokens( List.of( new NamedToken( "Label", 1 ) ) );
        srcTokenHolders.propertyKeyTokens().setInitialTokens( List.of( new NamedToken( "prop", 2 ), new NamedToken( "bla", 3 ) ) );
        when( src.getAll() ).thenReturn( List.of( rule1, rule2 ) );

        RecordStorageMigrator.migrateSchemaRules( srcTokenHolders, src, dst );

        assertEquals( 2, writtenRules.size() );
        Set<String> names = writtenRules.stream().map( SchemaRule::getName ).collect( Collectors.toSet() );

        // Collisions in generated names (however fantastically unlikely) must be resolved by appending a count.
        assertEquals( Set.of( "constraint_952591e6", "constraint_952591e6_1" ), names );
    }
}

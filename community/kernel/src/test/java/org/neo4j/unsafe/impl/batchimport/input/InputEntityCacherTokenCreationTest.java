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
package org.neo4j.unsafe.impl.batchimport.input;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.store.format.RecordFormat;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.test.Randoms;
import org.neo4j.test.rule.RandomRule;

import static java.lang.Math.abs;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class InputEntityCacherTokenCreationTest
{

    private static final int SUPPORTED_NUMBER_OF_TOKENS = 10;
    private static final int UNSUPPORTED_NUMER_OF_TOKENS = SUPPORTED_NUMBER_OF_TOKENS + 1;
    private static final AtomicInteger uniqueIdGenerator = new AtomicInteger( 10 );
    private final ExpectedException expectedException = ExpectedException.none();
    private final RandomRule randomRule = new RandomRule();

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule( randomRule ).around( expectedException );

    @Test
    public void notAllowCreationOfUnsupportedNumberOfProperties() throws IOException
    {
        initExpectedException( SUPPORTED_NUMBER_OF_TOKENS );
        cacheNodeWithProperties( UNSUPPORTED_NUMER_OF_TOKENS, SUPPORTED_NUMBER_OF_TOKENS );
    }

    @Test
    public void allowCreationOfSupportedNumberOfProperties() throws IOException
    {
        cacheNodeWithProperties( SUPPORTED_NUMBER_OF_TOKENS, SUPPORTED_NUMBER_OF_TOKENS );
    }

    @Test
    public void notAllowCreationOfUnsupportedNumberOfGroups() throws IOException
    {
        initExpectedException( SUPPORTED_NUMBER_OF_TOKENS );
        cacheGroups( UNSUPPORTED_NUMER_OF_TOKENS, SUPPORTED_NUMBER_OF_TOKENS );
    }

    @Test
    public void allowCreationOfSupportedNumberOfGroups() throws IOException
    {
        cacheGroups( SUPPORTED_NUMBER_OF_TOKENS, SUPPORTED_NUMBER_OF_TOKENS );
    }

    @Test
    public void notAllowCreationOfUnsupportedNumberOfLabels() throws IOException
    {
        initExpectedException( SUPPORTED_NUMBER_OF_TOKENS );
        cacheLabels( UNSUPPORTED_NUMER_OF_TOKENS, SUPPORTED_NUMBER_OF_TOKENS );
    }

    @Test
    public void allowCreationOfSupportedNumberOfLabels() throws IOException
    {
        cacheLabels( SUPPORTED_NUMBER_OF_TOKENS, SUPPORTED_NUMBER_OF_TOKENS );
    }

    @Test
    public void notAllowCreationOfUnsupportedNumberOfRelationshipTypes() throws IOException
    {
        initExpectedException( SUPPORTED_NUMBER_OF_TOKENS );
        cacheRelationship( UNSUPPORTED_NUMER_OF_TOKENS, SUPPORTED_NUMBER_OF_TOKENS );
    }

    @Test
    public void allowCreationOfSupportedNumberOfRelationshipTypes() throws IOException
    {
        cacheRelationship( SUPPORTED_NUMBER_OF_TOKENS, SUPPORTED_NUMBER_OF_TOKENS );
    }

    private void cacheRelationship( int iterations, int maxNumberOfRelationshipTypes ) throws IOException
    {
        RecordFormats recordFormats = mockRecordFormats( 1000, 1000, maxNumberOfRelationshipTypes, 1000 );
        try ( InputRelationshipCacheWriter cacher = getRelationshipCacher( recordFormats );
              InputEntityVisitor visitor = cacher.wrap( new InputEntity() ) )
        {
            for ( int i = 0; i < iterations; i++ )
            {
                generateRelationship( getRandoms(), visitor );
            }
        }
    }

    private void cacheLabels( int iterations, int maxNumberOfLabels ) throws IOException
    {
        RecordFormats recordFormats = mockRecordFormats( 1000, maxNumberOfLabels, 1000, 1000 );

        try ( InputNodeCacheWriter cacher = getNodeCacher( recordFormats );
              InputEntityVisitor visitor = cacher.wrap( new InputEntity() ) )
        {
            for ( int i = 0; i < iterations; i++ )
            {
                generateNode( getRandoms(), visitor );
            }
        }
    }

    private void cacheGroups( int iterations, int maxNumberOfGroups ) throws IOException
    {
        RecordFormats recordFormats = mockRecordFormats( 1000, 1000, 1000, maxNumberOfGroups );

        try ( InputNodeCacheWriter cacher = getNodeCacher( recordFormats );
              InputEntityVisitor visitor = cacher.wrap( new InputEntity() ) )
        {
            Randoms randoms = getRandoms();
            for ( int i = 0; i < iterations; i++ )
            {
                generateNode( randoms, visitor, false );
            }
        }
    }

    private void cacheNodeWithProperties( int iterations, int maxNumberOfProperties ) throws IOException
    {
        RecordFormats recordFormats = mockRecordFormats( maxNumberOfProperties, 1000, 1000, 1000 );

        try ( InputNodeCacheWriter cacher = getNodeCacher( recordFormats );
              InputEntityVisitor visitor = cacher.wrap( new InputEntity() ) )
        {
            Randoms randoms = getRandoms();
            for ( int i = 0; i < iterations; i++ )
            {
                generateNode( randoms, visitor );
            }
        }
    }

    private void initExpectedException( int numberOfSupportedTokens )
    {
        expectedException.expect( UnsupportedOperationException.class );
        expectedException.expectMessage( "Too many tokens. Creation of more then " + numberOfSupportedTokens + " " +
                                         "tokens is not supported." );
    }

    private void generateRelationship( Randoms randoms, InputEntityVisitor relationship ) throws IOException
    {
        generateProperties( randoms, relationship );
        relationship.startId( randomId( randoms ), generateGroup() );
        relationship.endId( randomId( randoms ), generateGroup() );
        relationship.type( getUniqueString() );
        relationship.endOfEntity();
    }

    private void generateNode( Randoms random, InputEntityVisitor node ) throws IOException
    {
        generateNode( random, node, true );
    }

    private void generateNode( Randoms random, InputEntityVisitor node, boolean propertiesAndLabels ) throws IOException
    {
        node.id( randomId( random ), generateGroup() );
        if ( propertiesAndLabels )
        {
            generateProperties( random, node );
            node.labels( randomLabels() );
        }
        node.endOfEntity();
    }

    private Group generateGroup()
    {
        return new Group.Adapter( uniqueIdGenerator.getAndIncrement(), getUniqueString() );
    }

    private String[] randomLabels()
    {
        String[] labels = new String[1];
        for ( int i = 0; i < labels.length; i++ )
        {
            labels[i] = getUniqueString();
        }
        return labels;
    }

    private String getUniqueString()
    {
        return uniqueIdGenerator.getAndIncrement() + "";
    }

    private void generateProperties( Randoms random, InputEntityVisitor entity )
    {
        int length = 1;
        for ( int i = 0; i < length; i++ )
        {
            String key = getUniqueString();
            String value = random.propertyValue() + "";
            entity.property( key, value );
        }
    }

    private Object randomId( Randoms random )
    {
        return abs( random.random().nextLong() );
    }

    private RecordFormats mockRecordFormats( long maxPropertyKeyId, long maxLabelId, long maxRelationshipTypeId,
            long maxRelationshipGroupId )
    {
        RecordFormats recordFormats = mock( RecordFormats.class );
        RecordFormat propertyKeyTokenFormat = getRecordFormatMock( maxPropertyKeyId );
        RecordFormat labelTokenFormat = getRecordFormatMock( maxLabelId );
        RecordFormat relationshipTypeTokenFormat = getRecordFormatMock( maxRelationshipTypeId );
        RecordFormat relationshipGroupTokenFormat = getRecordFormatMock( maxRelationshipGroupId );
        when( recordFormats.propertyKeyToken() ).thenReturn( propertyKeyTokenFormat );
        when( recordFormats.labelToken() ).thenReturn( labelTokenFormat );
        when( recordFormats.relationshipTypeToken() ).thenReturn( relationshipTypeTokenFormat );
        when( recordFormats.relationshipGroup() ).thenReturn( relationshipGroupTokenFormat );
        return recordFormats;
    }

    private RecordFormat getRecordFormatMock( long maxId )
    {
        RecordFormat recordFormat = mock( RecordFormat.class );
        when( recordFormat.getMaxId() ).thenReturn( maxId );
        return recordFormat;
    }

    private Randoms getRandoms()
    {
        return new Randoms( randomRule.random(), Randoms.DEFAULT );
    }

    private InputNodeCacheWriter getNodeCacher( RecordFormats recordFormats )
    {
        return new InputNodeCacheWriter( mock( StoreChannel.class ),
                mock( StoreChannel.class ), recordFormats, 100 );
    }

    private InputRelationshipCacheWriter getRelationshipCacher( RecordFormats recordFormats )
    {
        return new InputRelationshipCacheWriter( mock( StoreChannel.class ),
                mock( StoreChannel.class ), recordFormats, 100 );
    }
}

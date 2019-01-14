/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.tools.dump;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptorFactory;
import org.neo4j.kernel.impl.core.RelationshipTypeToken;
import org.neo4j.kernel.impl.store.LabelTokenStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.PropertyKeyTokenStore;
import org.neo4j.kernel.impl.store.RelationshipTypeTokenStore;
import org.neo4j.kernel.impl.store.SchemaStorage;
import org.neo4j.kernel.impl.store.kvstore.BigEndianByteArrayBuffer;
import org.neo4j.kernel.impl.store.kvstore.HeaderField;
import org.neo4j.kernel.impl.store.kvstore.Headers;
import org.neo4j.kernel.impl.store.kvstore.ReadableBuffer;
import org.neo4j.kernel.impl.store.kvstore.WritableBuffer;
import org.neo4j.kernel.impl.store.record.IndexRule;
import org.neo4j.storageengine.api.Token;
import org.neo4j.test.rule.SuppressOutput;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DumpCountsStoreTest
{
    private static final int START_LABEL_ID = 1;
    private static final int END_LABEL_ID = 2;
    private static final int INDEX_LABEL_ID = 3;
    private static final int NODE_LABEL_ID = 10;
    private static final String TEST_LABEL = "testLabel";
    private static final String START_LABEL = "startLabel";
    private static final String END_LABEL = "endLabel";
    private static final String INDEX_LABEL = "indexLabel";

    private static final int TYPE_ID = 1;
    private static final String TYPE_LABEL = "testType";

    private static final int INDEX_PROPERTY_KEY_ID = 1;
    private static final String INDEX_PROPERTY = "indexProperty";

    private static final long indexId = 0;
    private static final SchemaIndexDescriptor descriptor =
            SchemaIndexDescriptorFactory.forLabel( INDEX_LABEL_ID, INDEX_PROPERTY_KEY_ID );

    @Rule
    public SuppressOutput suppressOutput = SuppressOutput.suppressAll();

    @Test
    public void dumpMetadata()
    {
        DumpCountsStore countsStore = getCountStore();

        Headers headers = Headers.headersBuilder()
                .put( createNamedHeader( "header1" ), "value1" )
                .put( createNamedHeader( "header2" ), "value2" )
                .headers();
        File file = mock( File.class );
        when( file.toString() ).thenReturn( "Test File" );

        countsStore.visitMetadata( file, headers, 100 );
        assertThat( suppressOutput.getOutputVoice().toString(),
                allOf( containsString( "Counts Store:\tTest File" ),
                        containsString( "header2:\tvalue2" ),
                        containsString( "header1:\tvalue1" ),
                        containsString( "entries:\t100" ) ) );
    }

    @Test
    public void dumpNodeCount()
    {
        DumpCountsStore countsStore = getCountStore();
        countsStore.visitNodeCount( NODE_LABEL_ID, 70 );
        assertThat( suppressOutput.getOutputVoice().toString(),
                containsString( "Node[(:testLabel [labelId=10])]:\t70" ) );
    }

    @Test
    public void dumpRelationshipCount()
    {
        DumpCountsStore countsStore = getCountStore();
        countsStore.visitRelationshipCount( START_LABEL_ID, TYPE_ID, END_LABEL_ID, 5 );
        assertThat( suppressOutput.getOutputVoice().toString(),
                containsString( "\tRelationship[(:startLabel [labelId=1])-[:testType [typeId=1]]->" +
                                  "(:endLabel [labelId=2])]:\t5" ) );
    }

    @Test
    public void dumpUnknownKey()
    {
        DumpCountsStore countsStore = getCountStore();
        countsStore.visitUnknownKey( new BigEndianByteArrayBuffer( "unknownKey".getBytes() ),
                new BigEndianByteArrayBuffer( "unknownValue".getBytes() ) );
        assertThat( suppressOutput.getOutputVoice().toString(),
                containsString( "['u', 'n', 'k', 'n', 'o', 'w', 'n', 'K', 'e', 'y']:\t['u', 'n', 'k', 'n', 'o', " +
                                  "'w', 'n', 'V', 'a', 'l', 'u', 'e']" ) );
    }

    @Test
    public void dumpIndexStatistic()
    {
        DumpCountsStore countsStore = getCountStore();
        countsStore.visitIndexStatistics( indexId, 3, 4 );
        assertThat( suppressOutput.getOutputVoice().toString(),
                containsString( "IndexStatistics[(:indexLabel [labelId=3] {indexProperty [keyId=1]})" +
                                    "]:\tupdates=3, size=4" ) );
    }

    @Test
    public void dumpIndexSample()
    {
        DumpCountsStore countsStore = getCountStore();
        countsStore.visitIndexSample( indexId, 1, 2 );
        assertThat( suppressOutput.getOutputVoice().toString(),
                containsString( "IndexSample[(:indexLabel [labelId=3] {indexProperty [keyId=1]})]:\tunique=1, size=2" ));
    }

    private DumpCountsStore getCountStore()
    {
        return new DumpCountsStore( System.out, createNeoStores(), createSchemaStorage() );
    }

    private SchemaStorage createSchemaStorage()
    {
        SchemaStorage schemaStorage = mock(SchemaStorage.class);
        IndexProvider.Descriptor providerDescriptor = new IndexProvider.Descriptor( "in-memory", "1.0" );
        IndexRule rule = IndexRule.indexRule( indexId, descriptor, providerDescriptor );
        ArrayList<IndexRule> rules = new ArrayList<>();
        rules.add( rule );

        when( schemaStorage.indexesGetAll() ).thenReturn( rules.iterator() );
        return schemaStorage;
    }

    private NeoStores createNeoStores()
    {
        NeoStores neoStores = mock( NeoStores.class );
        LabelTokenStore labelTokenStore = mock( LabelTokenStore.class );
        RelationshipTypeTokenStore typeTokenStore = mock( RelationshipTypeTokenStore.class );
        PropertyKeyTokenStore propertyKeyTokenStore = mock( PropertyKeyTokenStore.class );

        when( labelTokenStore.getTokens( Integer.MAX_VALUE ) ).thenReturn( getLabelTokens() );
        when( typeTokenStore.getTokens( Integer.MAX_VALUE ) ).thenReturn( getTypeTokes() );
        when( propertyKeyTokenStore.getTokens( Integer.MAX_VALUE ) ).thenReturn( getPropertyTokens() );

        when( neoStores.getLabelTokenStore() ).thenReturn( labelTokenStore );
        when( neoStores.getRelationshipTypeTokenStore() ).thenReturn( typeTokenStore );
        when( neoStores.getPropertyKeyTokenStore() ).thenReturn( propertyKeyTokenStore );

        return neoStores;
    }

    private List<Token> getPropertyTokens()
    {
        return Collections.singletonList( new Token( INDEX_PROPERTY, INDEX_PROPERTY_KEY_ID ) );
    }

    private List<RelationshipTypeToken> getTypeTokes()
    {
        return Collections.singletonList( new RelationshipTypeToken( TYPE_LABEL, TYPE_ID ) );
    }

    private List<Token> getLabelTokens()
    {
        return Arrays.asList( new Token( START_LABEL, START_LABEL_ID ),
                new Token( END_LABEL, END_LABEL_ID ),
                new Token( INDEX_LABEL, INDEX_LABEL_ID ),
                new Token( TEST_LABEL, NODE_LABEL_ID ) );
    }

    private HeaderField<String> createNamedHeader( String name )
    {
        return new HeaderField<String>()
        {
            @Override
            public String read( ReadableBuffer header )
            {
                return name;
            }

            @Override
            public void write( String s, WritableBuffer header )
            {

            }

            @Override
            public String toString()
            {
                return name;
            }
        };
    }
}

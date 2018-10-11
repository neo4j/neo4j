/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.kernel.impl.store;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.id.DefaultIdGeneratorFactory;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.junit.jupiter.api.Assertions.assertEquals;

@PageCacheExtension
class ShortStringPropertyEncodeTest
{
    private static final int KEY_ID = 0;

    @Inject
    private TestDirectory testDirectory;
    @Inject
    private FileSystemAbstraction fileSystem;
    @Inject
    private PageCache pageCache;

    private NeoStores neoStores;
    private PropertyStore propertyStore;

    @BeforeEach
    void setupStore()
    {
        neoStores = new StoreFactory( testDirectory.databaseLayout(), Config.defaults(), new DefaultIdGeneratorFactory( fileSystem ),
                pageCache, fileSystem, NullLogProvider.getInstance(), EmptyVersionContextSupplier.EMPTY ).openNeoStores( true,
                StoreType.PROPERTY, StoreType.PROPERTY_ARRAY, StoreType.PROPERTY_STRING );
        propertyStore = neoStores.getPropertyStore();
    }

    @AfterEach
    void closeStore()
    {
        neoStores.close();
    }

    @Test
    void canEncodeEmptyString()
    {
        assertCanEncode( "" );
    }

    @Test
    void canEncodeReallyLongString()
    {
        assertCanEncode( "                    " ); // 20 spaces
        assertCanEncode( "                " ); // 16 spaces
    }

    @Test
    void canEncodeFifteenSpaces()
    {
        assertCanEncode( "               " );
    }

    @Test
    void canEncodeNumericalString()
    {
        assertCanEncode( "0123456789+,'.-" );
        assertCanEncode( " ,'.-0123456789" );
        assertCanEncode( "+ '.0123456789-" );
        assertCanEncode( "+, 0123456789.-" );
        assertCanEncode( "+,0123456789' -" );
        assertCanEncode( "+0123456789,'. " );
        // IP(v4) numbers
        assertCanEncode( "192.168.0.1" );
        assertCanEncode( "127.0.0.1" );
        assertCanEncode( "255.255.255.255" );
    }

    @Test
    void canEncodeTooLongStringsWithCharsInDifferentTables()
    {
        assertCanEncode( "____________+" );
        assertCanEncode( "_____+_____" );
        assertCanEncode( "____+____" );
        assertCanEncode( "HELLO world" );
        assertCanEncode( "Hello_World" );
    }

    @Test
    void canEncodeUpToNineEuropeanChars()
    {
        // Shorter than 10 chars
        assertCanEncode( "fågel" ); // "bird" in Swedish
        assertCanEncode( "påfågel" ); // "peacock" in Swedish
        assertCanEncode( "påfågelö" ); // "peacock island" in Swedish
        assertCanEncode( "påfågelön" ); // "the peacock island" in Swedish
        // 10 chars
        assertCanEncode( "påfågelöar" ); // "peacock islands" in Swedish
    }

    @Test
    void canEncodeEuropeanCharsWithPunctuation()
    {
        assertCanEncode( "qHm7 pp3" );
        assertCanEncode( "UKKY3t.gk" );
    }

    @Test
    void canEncodeAlphanumerical()
    {
        assertCanEncode( "1234567890" ); // Just a sanity check
        assertCanEncodeInBothCasings( "HelloWor1d" ); // There is a number there
        assertCanEncode( "          " ); // Alphanum is the first that can encode 10 spaces
        assertCanEncode( "_ _ _ _ _ " ); // The only available punctuation
        assertCanEncode( "H3Lo_ or1D" ); // Mixed case + punctuation
        assertCanEncode( "q1w2e3r4t+" ); // + is not in the charset
    }

    @Test
    void canEncodeHighUnicode()
    {
        assertCanEncode( "\u02FF" );
        assertCanEncode( "hello\u02FF" );
    }

    @Test
    void canEncodeLatin1SpecialChars()
    {
        assertCanEncode( "#$#$#$#" );
        assertCanEncode( "$hello#" );
    }

    @Test
    void canEncodeTooLongLatin1String()
    {
        assertCanEncode( "#$#$#$#$" );
    }

    @Test
    void canEncodeLowercaseAndUppercaseStringsUpTo12Chars()
    {
        assertCanEncodeInBothCasings( "hello world" );
        assertCanEncode( "hello_world" );
        assertCanEncode( "_hello_world" );
        assertCanEncode( "hello::world" );
        assertCanEncode( "hello//world" );
        assertCanEncode( "hello world" );
        assertCanEncode( "http://ok" );
        assertCanEncode( "::::::::" );
        assertCanEncode( " _.-:/ _.-:/" );
    }

    private void assertCanEncodeInBothCasings( String string )
    {
        assertCanEncode( string.toLowerCase() );
        assertCanEncode( string.toUpperCase() );
    }

    private void assertCanEncode( String string )
    {
        encode( string );
    }

    private void encode( String string )
    {
        PropertyBlock block = new PropertyBlock();
        TextValue expectedValue = Values.stringValue( string );
        propertyStore.encodeValue( block, KEY_ID, expectedValue );
        assertEquals( 0, block.getValueRecords().size() );
        Value readValue = block.getType().value( block, propertyStore );
        assertEquals( expectedValue, readValue );
    }
}

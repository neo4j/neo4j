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
package org.neo4j.index.internal.gbptree;

import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.neo4j.cursor.RawCursor;
import org.neo4j.io.compress.ZipUtils;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.index.internal.gbptree.SimpleLongLayout.longLayout;
import static org.neo4j.test.rule.PageCacheRule.config;

/**
 * A little trick to automatically tell whether or not index format was changed without
 * incrementing the format version. This is done by keeping a zipped tree which is opened and tested on.
 * On failure this test will fail saying that the format version needs update and also update the zipped
 * store with the new version.
 */
@RunWith( Parameterized.class )
public class FormatCompatibilityTest
{
    private static final String STORE = "store";
    private static final int INITIAL_KEY_COUNT = 10_000;
    private static final String CURRENT_FIXED_SIZE_FORMAT_ZIP = "current-format.zip";
    private static final String CURRENT_DYNAMIC_SIZE_FORMAT_ZIP = "current-dynamic-format.zip";

    @Parameters
    public static List<Object[]> data()
    {
        return asList(
                new Object[] {longLayout().withFixedSize( true ).build(), CURRENT_FIXED_SIZE_FORMAT_ZIP},
                new Object[] {longLayout().withFixedSize( false ).build(), CURRENT_DYNAMIC_SIZE_FORMAT_ZIP} );
    }

    @Parameter
    public SimpleLongLayout layout;
    @Parameter( 1 )
    public String zipName;

    private final TestDirectory directory = TestDirectory.testDirectory( getClass() );
    private final PageCacheRule pageCacheRule = new PageCacheRule( config().withInconsistentReads( false ) );
    private final DefaultFileSystemRule fsRule = new DefaultFileSystemRule();
    private final RandomRule random = new RandomRule();

    @Rule
    public final RuleChain chain = RuleChain.outerRule( random ).around( fsRule ).around( directory ).around( pageCacheRule );

    @Test
    public void shouldDetectFormatChange() throws Throwable
    {
        List<Long> initialKeys = initialKeys();
        List<Long> keysToAdd = keysToAdd();
        List<Long> allKeys = new ArrayList<>();
        allKeys.addAll( initialKeys );
        allKeys.addAll( keysToAdd );
        allKeys.sort( Long::compare );

        // GIVEN stored tree
        File storeFile = directory.file( STORE );
        try
        {
            unzipTo( storeFile );
        }
        catch ( FileNotFoundException e )
        {
            // First time this test is run, eh?
            createAndZipTree( storeFile );
            tellDeveloperToCommitThisFormatVersion();
        }
        assertTrue( zipName + " seems to be missing from resources directory", fsRule.get().fileExists( storeFile ) );

        PageCache pageCache = pageCacheRule.getPageCache( fsRule.get() );
        try ( GBPTree<MutableLong,MutableLong> tree =
                new GBPTreeBuilder<>( pageCache, storeFile, layout ).build() )
        {
            try
            {
                {
                    // WHEN reading from the tree
                    // THEN initial keys should be there
                    tree.consistencyCheck();
                    try ( RawCursor<Hit<MutableLong,MutableLong>,IOException> cursor =
                                  tree.seek( layout.key( 0 ), layout.key( Long.MAX_VALUE ) ) )
                    {
                        for ( Long expectedKey : initialKeys )
                        {
                            assertHit( cursor, expectedKey );
                        }
                        assertFalse( cursor.next() );
                    }
                }

                {
                    // WHEN writing more to the tree
                    // THEN we should not see any format conflicts
                    try ( Writer<MutableLong,MutableLong> writer = tree.writer() )
                    {
                        while ( keysToAdd.size() > 0 )
                        {
                            int next = random.nextInt( keysToAdd.size() );
                            put( writer, keysToAdd.get( next ) );
                            keysToAdd.remove( next );
                        }
                    }
                }

                {
                    // WHEN reading from the tree again
                    // THEN all keys including newly added should be there
                    tree.consistencyCheck();
                    try ( RawCursor<Hit<MutableLong,MutableLong>,IOException> cursor =
                                  tree.seek( layout.key( 0 ), layout.key( 2 * INITIAL_KEY_COUNT ) ) )
                    {
                        for ( Long expectedKey : allKeys )
                        {
                            assertHit( cursor, expectedKey );
                        }
                        assertFalse( cursor.next() );
                    }
                }

                {
                    // WHEN randomly removing half of tree content
                    // THEN we should not see any format conflicts
                    try ( Writer<MutableLong,MutableLong> writer = tree.writer() )
                    {
                        int size = allKeys.size();
                        while ( allKeys.size() > size / 2 )
                        {
                            int next = random.nextInt( allKeys.size() );
                            MutableLong key = layout.key( allKeys.get( next ) );
                            writer.remove( key );
                            allKeys.remove( next );
                        }
                    }
                }

                {
                    // WHEN reading from the tree after remove
                    // THEN we should see everything that is left in the tree
                    tree.consistencyCheck();
                    try ( RawCursor<Hit<MutableLong,MutableLong>,IOException> cursor =
                                  tree.seek( layout.key( 0 ), layout.key( 2 * INITIAL_KEY_COUNT ) ) )
                    {
                        for ( Long expectedKey : allKeys )
                        {
                            assertHit( cursor, expectedKey );
                        }
                        assertFalse( cursor.next() );
                    }
                }
            }
            catch ( Throwable t )
            {
                throw new AssertionError( "If this is the single failing test in this component then this failure is a strong indication that format " +
                        "has changed without also incrementing TreeNode version(s). Please make necessary format version changes.", t );
            }
        }
        catch ( MetadataMismatchException e )
        {
            // Good actually, or?
            assertThat( e.getMessage(), containsString( "format version" ) );

            fsRule.get().deleteFile( storeFile );
            createAndZipTree( storeFile );

            tellDeveloperToCommitThisFormatVersion();
        }
    }

    private void tellDeveloperToCommitThisFormatVersion()
    {
        fail( format( "This is merely a notification to developer. Format has changed and its version has also " +
                        "been properly incremented. A tree with this new format has been generated and should be committed. " +
                        "Please move:%n  %s%ninto %n  %s, %nreplacing the existing file there",
                directory.file( zipName ),
                "<index-module>" + pathify( ".src.test.resources." ) +
                        pathify( getClass().getPackage().getName() + "." ) + zipName ) );
    }

    private static String pathify( String name )
    {
        return name.replace( '.', File.separatorChar );
    }

    private void unzipTo( File storeFile ) throws IOException
    {
        URL resource = getClass().getResource( zipName );
        if ( resource == null )
        {
            throw new FileNotFoundException();
        }

        try ( ZipFile zipFile = new ZipFile( resource.getFile() ) )
        {
            Enumeration<? extends ZipEntry> entries = zipFile.entries();
            assertTrue( entries.hasMoreElements() );
            ZipEntry entry = entries.nextElement();
            assertEquals( STORE, entry.getName() );
            Files.copy( zipFile.getInputStream( entry ), storeFile.toPath() );
        }
    }

    private void createAndZipTree( File storeFile ) throws IOException
    {
        List<Long> initialKeys = initialKeys();
        PageCache pageCache = pageCacheRule.getPageCache( fsRule.get() );
        try ( GBPTree<MutableLong,MutableLong> tree =
                      new GBPTreeBuilder<>( pageCache, storeFile, layout ).build() )
        {
            try ( Writer<MutableLong,MutableLong> writer = tree.writer() )
            {
                for ( Long key : initialKeys )
                {
                    put( writer, key );
                }
            }
            tree.checkpoint( IOLimiter.unlimited() );
        }
        ZipUtils.zip( fsRule.get(), storeFile, directory.file( zipName ) );
    }

    private static long value( long key )
    {
        return key * 2;
    }

    private List<Long> initialKeys()
    {
        List<Long> initialKeys = new ArrayList<>();
        for ( long i = 0, key = 0; i < INITIAL_KEY_COUNT; i++, key += 2 )
        {
            initialKeys.add( key );
        }
        return initialKeys;
    }

    private List<Long> keysToAdd()
    {
        List<Long> keysToAdd = new ArrayList<>();
        for ( long i = 0, key = 1; i < INITIAL_KEY_COUNT; i++, key += 2 )
        {
            keysToAdd.add( key );
        }
        return keysToAdd;
    }

    private void assertHit( RawCursor<Hit<MutableLong,MutableLong>,IOException> cursor, Long expectedKey ) throws IOException
    {
        assert cursor.next() : "Had no next when expecting key " + expectedKey;
        Hit<MutableLong,MutableLong> hit = cursor.get();
        assertEquals( expectedKey.longValue(), hit.key().longValue() );
        assertEquals( value( expectedKey ), hit.value().longValue() );
    }

    private void put( Writer<MutableLong,MutableLong> writer, long key ) throws IOException
    {
        MutableLong insertKey = layout.key( key );
        MutableLong insertValue = layout.value( value( key ) );
        writer.put( insertKey, insertValue );
    }
}

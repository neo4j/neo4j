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
package org.neo4j.kernel.impl.index;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.index.labelscan.GBPTreePageCacheFileUtil;
import org.neo4j.kernel.impl.index.schema.GBPTreeFileSystemFileUtil;
import org.neo4j.test.rule.PageCacheAndDependenciesRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith( Parameterized.class )
public class GBPTreeFileUtilTest extends AbstractGBPTreeFileUtilTest
{
    @ClassRule
    public static PageCacheAndDependenciesRule pageCacheAndDependenciesRule = new PageCacheAndDependenciesRule(
            DefaultFileSystemRule::new, GBPTreeFileUtilTest.class
    );

    private static FileSystemAbstraction fs;
    private static TestDirectory directory;

    @BeforeClass
    public static void extractFileSystem()
    {
        fs = pageCacheAndDependenciesRule.fileSystem();
        directory = pageCacheAndDependenciesRule.directory();
    }

    @Parameterized.Parameters( name = "{0}" )
    public static Collection<GBPTreeFileUtil> fileUtils()
    {
        return Arrays.asList(
                new GBPTreePageCacheFileUtil( pageCacheAndDependenciesRule.pageCache() ),
                new GBPTreeFileSystemFileUtil( pageCacheAndDependenciesRule.fileSystem() ) );
    }

    @Parameterized.Parameter
    public GBPTreeFileUtil gbpTreeFileUtil;

    @Override
    protected GBPTreeFileUtil getGBPTreeFileUtil()
    {
        return gbpTreeFileUtil;
    }

    @Override
    protected File existingFile( String fileName ) throws IOException
    {
        File file = directory.file( fileName );
        fs.create( file ).close();
        return file;
    }

    @Override
    protected File nonExistingFile( String fileName )
    {
        return directory.file( fileName );
    }

    @Override
    protected File nonExistingDirectory( String directoryName )
    {
        return new File( directory.absolutePath(), directoryName );
    }

    @Override
    protected void assertFileDoesNotExist( File file )
    {
        assertFalse( fs.fileExists( file ) );
    }

    @Override
    protected void assertDirectoryExist( File directory )
    {
        assertTrue( fs.fileExists( directory ) );
        assertTrue( fs.isDirectory( directory ) );
    }
}

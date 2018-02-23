/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.commandline.dbms;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.annotation.Resource;

import org.neo4j.commandline.Util;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.commandline.Util.isSameOrChildFile;
import static org.neo4j.commandline.Util.isSameOrChildPath;
import static org.neo4j.commandline.Util.neo4jVersion;

@ExtendWith( TestDirectoryExtension.class )
public class UtilTest
{
    @Resource
    public TestDirectory directory;

    @Test
    public void canonicalPath()
    {
        assertNotNull( Util.canonicalPath( "foo" ).getParent() );
    }

    @Test
    public void returnsAVersion()
    {
        assertNotNull( neo4jVersion(), "A version should be returned" );
    }

    @Test
    public void correctlyIdentifySameOrChildFile()
    {
        assertTrue( isSameOrChildFile( directory.directory(), directory.directory( "a" ) ) );
        assertTrue( isSameOrChildFile( directory.directory(), directory.directory() ) );
        assertTrue( isSameOrChildFile( directory.directory( "/a/./b" ), directory.directory( "a/b" ) ) );
        assertTrue( isSameOrChildFile( directory.directory( "a/b" ), directory.directory( "/a/./b" ) ) );

        assertFalse( isSameOrChildFile( directory.directory( "a" ), directory.directory( "b" ) ) );
    }

    @Test
    public void correctlyIdentifySameOrChildPath()
    {
        assertTrue( isSameOrChildPath( directory.directory().toPath(), directory.directory( "a" ).toPath() ) );
        assertTrue( isSameOrChildPath( directory.directory().toPath(), directory.directory().toPath() ) );
        assertTrue( isSameOrChildPath( directory.directory( "/a/./b" ).toPath(),
                directory.directory( "a/b" ).toPath() ) );
        assertTrue( isSameOrChildPath( directory.directory( "a/b" ).toPath(),
                directory.directory( "/a/./b" ).toPath() ) );

        assertFalse( isSameOrChildPath( directory.directory( "a" ).toPath(),
                directory.directory( "b" ).toPath() ) );
    }
}

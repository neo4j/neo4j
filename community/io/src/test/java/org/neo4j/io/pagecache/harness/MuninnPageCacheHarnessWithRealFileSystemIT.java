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
package org.neo4j.io.pagecache.harness;

import org.junit.jupiter.api.extension.ExtendWith;

import javax.annotation.Resource;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.pagecache.impl.muninn.MuninnPageCache;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

@ExtendWith( TestDirectoryExtension.class )
public class MuninnPageCacheHarnessWithRealFileSystemIT extends MuninnPageCacheHarnessTest
{
    @Resource
    public TestDirectory directory;

    @Override
    protected Fixture<MuninnPageCache> createFixture()
    {
        return super.createFixture()
                .withFileSystemAbstraction( DefaultFileSystemAbstraction::new )
                .withFileConstructor( pathname -> directory.file( pathname ) );
    }
}

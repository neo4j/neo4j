/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.impl.store;

import java.io.File;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.RuleChain;

import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.kernel.impl.store.id.IdGenerator;
import org.neo4j.kernel.impl.store.id.IdGeneratorImpl;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static org.junit.Assert.assertTrue;

public class IdGeneratorImplContractTest extends IdGeneratorContractTest
{
    private EphemeralFileSystemRule fsRule = new EphemeralFileSystemRule();
    private TestDirectory testDirectory = TestDirectory.testDirectory(fsRule.get());

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule( fsRule ).around( testDirectory );

    private EphemeralFileSystemAbstraction fs;

    @Before
    public void doBefore()
    {
        fs = fsRule.get();
    }

    @Override
    protected IdGenerator createIdGenerator( int grabSize )
    {
        IdGeneratorImpl.createGenerator( fs, idGeneratorFile(), 0, false );
        return openIdGenerator( grabSize );
    }

    @Override
    protected IdGenerator openIdGenerator( int grabSize )
    {
        return new IdGeneratorImpl( fs, idGeneratorFile(), grabSize, 1000, false, () -> 0L );
    }

    @After
    public void verifyFileCleanup() throws Exception
    {
        File file = idGeneratorFile();
        if ( file.exists() )
        {
            assertTrue( file.delete() );
        }
    }

    private File idGeneratorFile()
    {
        return testDirectory.file( "testIdGenerator.id" );
    }
}

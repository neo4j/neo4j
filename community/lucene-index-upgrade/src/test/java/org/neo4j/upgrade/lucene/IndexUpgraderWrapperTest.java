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
package org.neo4j.upgrade.lucene;

import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.file.Paths;

import org.neo4j.upgrade.loader.EmbeddedJarLoader;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class IndexUpgraderWrapperTest
{

    @Test
    public void indexUpgraderInvokesLuceneMigrator() throws Throwable
    {
        IndexUpgraderWrapper upgrader = getIndexUpgrader( createJarLoader() );

        UpgraterStub.resetInvocationMark();
        upgrader.upgradeIndex( Paths.get( "some" ) );
        assertTrue( UpgraterStub.getInvocationMark() );
    }

    @Test
    public void indexUpgraderReleaseResourcesOnClose() throws Throwable
    {
        EmbeddedJarLoader jarLoader = createJarLoader();
        IndexUpgraderWrapper upgrader = getIndexUpgrader( jarLoader );

        upgrader.upgradeIndex( Paths.get( "some" ) );
        upgrader.close();

        verify( jarLoader ).close();
    }

    private IndexUpgraderWrapper getIndexUpgrader( EmbeddedJarLoader jarLoader )
    {
        return new IndexUpgraderWrapper( () -> jarLoader );
    }

    private EmbeddedJarLoader createJarLoader() throws ClassNotFoundException, IOException
    {
        EmbeddedJarLoader jarLoader = Mockito.mock( EmbeddedJarLoader.class );
        when( jarLoader.loadEmbeddedClass( anyString() ) ).thenReturn( UpgraterStub.class );
        return jarLoader;
    }
}

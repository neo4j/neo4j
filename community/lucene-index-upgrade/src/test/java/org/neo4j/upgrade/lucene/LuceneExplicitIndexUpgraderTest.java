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

import org.hamcrest.Description;
import org.hamcrest.Matchers;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Supplier;

import org.neo4j.upgrade.loader.EmbeddedJarLoader;
import org.neo4j.upgrade.loader.JarLoaderSupplier;

import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.neo4j.upgrade.lucene.LuceneExplicitIndexUpgrader.NO_MONITOR;

public class LuceneExplicitIndexUpgraderTest
{
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void failOnFileMigration() throws Exception
    {
        Path indexFolder = createPathForResource("indexPretender.txt");
        expectedException.expect( IllegalArgumentException.class );

        new LuceneExplicitIndexUpgrader( indexFolder, NO_MONITOR );
    }

    @Test
    public void ignoreFoldersWithoutIndexes() throws URISyntaxException, ExplicitIndexMigrationException
    {
        Path indexFolder = createPathForResource("notIndexFolder");
        TrackingLuceneExplicitIndexUpgrader indexUpgrader = new TrackingLuceneExplicitIndexUpgrader( indexFolder );
        indexUpgrader.upgradeIndexes();

        assertTrue(indexUpgrader.getMigratedIndexes().isEmpty());
    }

    @Test
    public void migrateValidIndexes() throws URISyntaxException, ExplicitIndexMigrationException
    {
        Path indexFolder = createPathForResource("indexFolder");
        TrackingLuceneExplicitIndexUpgrader indexUpgrader = new TrackingLuceneExplicitIndexUpgrader( indexFolder );
        indexUpgrader.upgradeIndexes();

        assertThat(indexUpgrader.getMigratedIndexes(), Matchers.contains("index1", "index2"));
    }

    @Test
    public void pointIncorrectIndexOnMigrationFailure() throws URISyntaxException, ExplicitIndexMigrationException
    {
        Path indexFolder = createPathForResource("indexFolder");
        TrackingLuceneExplicitIndexUpgrader indexUpgrader = new TrackingLuceneExplicitIndexUpgrader( indexFolder, true );

        expectedException.expect( ExplicitIndexMigrationException.class );
        expectedException.expect( new ExplicitIndexMigrationExceptionBaseMatcher( "index1", "index2" ) );

        indexUpgrader.upgradeIndexes();
    }

    private Path createPathForResource( String resourceName ) throws URISyntaxException
    {
        return Paths.get( getClass().getClassLoader().getResource( resourceName ).toURI() );
    }

    private static class ExplicitIndexMigrationExceptionBaseMatcher extends
            TypeSafeDiagnosingMatcher<ExplicitIndexMigrationException>
    {

        private final String[] failedIndexNames;

        ExplicitIndexMigrationExceptionBaseMatcher( String... failedIndexNames )
        {
            this.failedIndexNames = failedIndexNames;
        }

        @Override
        public void describeTo( Description description )
        {
            description.appendText( "Failed index should be one of:" )
                    .appendText( Arrays.toString( failedIndexNames ) );
        }

        @Override
        protected boolean matchesSafely( ExplicitIndexMigrationException item, Description mismatchDescription )
        {
            String brokendIndexName = item.getFailedIndexName();
            boolean matched = Arrays.asList(failedIndexNames).contains( brokendIndexName );
            if ( !matched )
            {
                mismatchDescription.appendText( "Failed index is: " ).appendText( brokendIndexName );
            }
            return matched;
        }
    }

    private static class TrackingLuceneExplicitIndexUpgrader extends LuceneExplicitIndexUpgrader
    {
        private final Set<String> migratedIndexes = new HashSet<>( );
        private final boolean failIndexUpgrade;

        TrackingLuceneExplicitIndexUpgrader( Path indexRootPath )
        {
            this ( indexRootPath, false );
        }

        TrackingLuceneExplicitIndexUpgrader( Path indexRootPath, boolean failIndexUpgrade )
        {
            super( indexRootPath, NO_MONITOR );
            this.failIndexUpgrade = failIndexUpgrade;
        }

        @Override
        IndexUpgraderWrapper createIndexUpgrader( String[] jars )
        {
            return new IndexUpgraderWrapperStub( JarLoaderSupplier.of( jars ), migratedIndexes, failIndexUpgrade );
        }

        public Set<String> getMigratedIndexes()
        {
            return migratedIndexes;
        }
    }

    private static class IndexUpgraderWrapperStub extends IndexUpgraderWrapper
    {

        private final Set<String> migratedIndexes;
        private final boolean failIndexUpgrade;

        IndexUpgraderWrapperStub( Supplier<EmbeddedJarLoader> jarLoaderSupplier, Set<String> migratedIndexes,
                boolean failIndexUpgrade )
        {
            super( jarLoaderSupplier );
            this.migratedIndexes = migratedIndexes;
            this.failIndexUpgrade = failIndexUpgrade;
        }

        @Override
        public void upgradeIndex( Path indexPath )
        {
            if ( failIndexUpgrade )
            {
                throw new RuntimeException( "Fail index migration: " + indexPath );
            }
            migratedIndexes.add( indexPath.getFileName().toString() );
        }
    }
}

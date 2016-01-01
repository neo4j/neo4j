/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package upgrade;

import java.io.File;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.impl.core.NonUniqueTokenException;
import org.neo4j.kernel.impl.storemigration.legacystore.LegacyStore;
import org.neo4j.kernel.lifecycle.LifecycleException;
import org.neo4j.test.CleanupRule;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.Unzip;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class FailToStartStoreMigratorIT
{
    @Test
    public void shouldFailToStartWithImproperlyUpgradedPropertyKeyStore() throws Exception
    {
        // given
        // a store with duplicate property keys, that was upgraded from 1.9 to 2.1.3, where no de-duplication was made
        Unzip.unzip( LegacyStore.class, "v21/upgradeMissedPropKeyDup.zip", storeDir );

        // when
        try
        {
            new GraphDatabaseFactory().newEmbeddedDatabase( storeDir.getAbsolutePath() );
            fail( "should have failed to start" );
        }
        // then
        catch ( RuntimeException e )
        {
            assertThat( e.getCause(), instanceOf( LifecycleException.class ) );
            Throwable root = e.getCause().getCause();
            assertThat( root, instanceOf( NonUniqueTokenException.class ) );
            assertNull( root.getCause() );
            assertTrue( root.getMessage().startsWith( "The PropertyKey \"name\" is not unique" ) );
        }
    }

    private final File storeDir = TargetDirectory.forTest( getClass() ).makeGraphDbDir();

    public final
    @Rule
    CleanupRule cleanup = new CleanupRule();
}

/**
 * Copyright (c) 2002-2012 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.backup.check;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.kernel.InternalAbstractGraphDatabase;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.impl.nioneo.store.AbstractBaseRecord;
import org.neo4j.kernel.impl.nioneo.store.RecordStore;
import org.neo4j.kernel.impl.nioneo.store.StoreAccess;
import org.neo4j.test.TargetDirectory;

public class TestConsistencyCheck
{
    private static class Inconsistency
    {
        private final AbstractBaseRecord record, referred;

        Inconsistency( AbstractBaseRecord record, AbstractBaseRecord referred )
        {
            this.record = record;
            this.referred = referred;
        }

        @Override
        public String toString()
        {
            if ( referred != null ) return record + " & " + referred;
            return record.toString();
        }
    }

    @Test
    public void cleanStoreDoesNotContainAnyInconsistencies()
    {
        assertTrue( check( ExpectedInconsistencies.NONE ).isEmpty() );
    }

    enum ExpectedInconsistencies
    {
        NONE( false ),
        SOME( true );
        private final boolean any;

        private ExpectedInconsistencies( boolean any )
        {
            this.any = any;
        }
    }
    
    private static void buildCleanDatabase( InternalAbstractGraphDatabase graphdb )
    {

    }
    
    private List<Inconsistency> check( ExpectedInconsistencies expect )
    {
        List<Inconsistency> report = new ArrayList<TestConsistencyCheck.Inconsistency>();
        ConsistencyCheck checker = checker( store, report );
        boolean foundInconsistencies = false;
        try
        {
            checker.run();
        }
        catch ( AssertionError e )
        {
            foundInconsistencies = true;
        }
        if ( expect.any != foundInconsistencies )
        {
            fail( expect.any ? "expected inconsistencies, but none found"
                    : "found inconsistencies despite none being expected" );
        }
        return report;
    }

    private static ConsistencyCheck checker( StoreAccess stores, final Collection<Inconsistency> report )
    {
        return new ConsistencyCheck( stores )
        {
            @Override
            protected <R extends AbstractBaseRecord> void report( RecordStore<R> recordStore, R record, InconsistencyType inconsistency )
            {
                report.add( new Inconsistency( record, null ) );
            }

            @Override
            protected <R1 extends AbstractBaseRecord, R2 extends AbstractBaseRecord> void report(
                    RecordStore<R1> recordStore, R1 record, RecordStore<? extends R2> referredStore, R2 referred,
                    InconsistencyType inconsistency )
            {
                report.add( new Inconsistency( record, referred ) );
            }
        };
    }

    @Rule
    public final TargetDirectory.TestDirectory test = TargetDirectory.testDirForTest( TestConsistencyCheck.class );
    private InternalAbstractGraphDatabase graphdb;
    private StoreAccess store;

    @Before
    public void startGraphDatabase()
    {
        graphdb = new EmbeddedGraphDatabase( test.directory().getAbsolutePath() );
        store = new StoreAccess( graphdb );
        buildCleanDatabase( graphdb );
    }

    @After
    public void stopGraphDatabase()
    {
        store = null;
        try
        {
            if ( graphdb != null ) graphdb.shutdown();
        }
        finally
        {
            graphdb = null;
        }
    }
}

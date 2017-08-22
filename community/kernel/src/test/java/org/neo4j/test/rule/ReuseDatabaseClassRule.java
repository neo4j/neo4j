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
package org.neo4j.test.rule;

import java.util.function.Supplier;

/**
 * This test rule allows tests to define a database rule as a non-class rule, but elevate its life cycle to that of a
 * class rule.
 * <p>
 * This can make tests run significantly faster, iff it can be guaranteed that tests will not need to change the
 * database configuration, and will not be influenced by any existing data in the database.
 * <p>
 * The database will be instantiated by the first test that runs, and will be reused by all subsequent tests running in
 * that test class.
 * <p>
 * <strong>Note</strong> that you only need to use this class rule if you cannot make the {@link DatabaseRule} itself
 * annotated with {@link org.junit.ClassRule} for some reason.
 */
public class ReuseDatabaseClassRule extends ExternalResource
{
    private DatabaseRule original;
    private ExternalResource lifecycled = new ExternalResource()
    {
        private boolean started;

        @Override
        protected void before() throws Throwable
        {
            if ( !started )
            {
                original.before();
                started = true;
            }
        }
    };

    @Override
    protected void after( boolean successful ) throws Throwable
    {
        original.after( successful );
        original = null;
    }

    public DatabaseRule getOrCreate( Supplier<DatabaseRule> createDatabaseRule )
    {
        if ( original == null )
        {
            original = createDatabaseRule.get();
        }
        return original;
    }

    public ExternalResource getRule()
    {
        return lifecycled;
    }
}

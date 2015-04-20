/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.api.index;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(ParameterizedSuiteRunner.class)
@Suite.SuiteClasses({
        NonUniqueIndexPopulatorCompatibility.class,
        UniqueIndexPopulatorCompatibility.class,
        UniqueIndexAccessorCompatibility.class,
        UniqueConstraintCompatibility.class
})
public abstract class IndexProviderCompatibilityTestSuite
{
    protected abstract SchemaIndexProvider createIndexProvider();

    public static abstract class Compatibility
    {
        protected final SchemaIndexProvider indexProvider;
        protected IndexDescriptor descriptor = new IndexDescriptor( 1, 2 );

        public Compatibility( IndexProviderCompatibilityTestSuite testSuite )
        {
            this.indexProvider = testSuite.createIndexProvider();
        }
    }
}

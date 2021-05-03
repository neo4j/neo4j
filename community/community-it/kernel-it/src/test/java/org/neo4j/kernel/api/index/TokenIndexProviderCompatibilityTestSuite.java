/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.kernel.api.index;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import org.neo4j.annotations.documented.ReporterFactories;
import org.neo4j.common.EntityType;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.kernel.impl.index.schema.ConsistencyCheckable;
import org.neo4j.test.runner.ParameterizedSuiteRunner;

import static org.neo4j.io.pagecache.context.CursorContext.NULL;

@RunWith( ParameterizedSuiteRunner.class )
@Suite.SuiteClasses( {
        TokenIndexPopulatorCompatibility.class
} )
public abstract class TokenIndexProviderCompatibilityTestSuite extends IndexProviderCompatibilityTestSuite
{
    @Override
    protected IndexPrototype indexPrototype()
    {
        return IndexPrototype.forSchema( SchemaDescriptor.forAnyEntityTokens( EntityType.NODE ) );
    }

    @Override
    public void consistencyCheck( IndexPopulator populator )
    {
        ((ConsistencyCheckable) populator).consistencyCheck( ReporterFactories.throwingReporterFactory(), NULL );
    }

    public abstract static class Compatibility extends IndexProviderCompatibilityTestSuite.Compatibility
    {
        final TokenIndexProviderCompatibilityTestSuite testSuite;

        Compatibility( TokenIndexProviderCompatibilityTestSuite testSuite, IndexPrototype prototype )
        {
            super( testSuite, prototype );
            this.testSuite = testSuite;
        }
    }
}

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
package org.neo4j.performance.domain.benchmark;

import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;

import org.reflections.Reflections;

/**
 * Used to find benchmark cases to run.
 */
public class BenchmarkLoader
{

    public Collection<Benchmark> loadBenchmarksIn( String rootPackageName )
    {
        Reflections reflections = new Reflections(rootPackageName);

        Set<Class<? extends Benchmark>> caseClasses =
                reflections.getSubTypesOf(Benchmark.class);

        // Instantiate cases
        Collection<Benchmark> cases = new ArrayList<Benchmark>();

        for ( Class<? extends Benchmark> caseClass : caseClasses )
        {
            try
            {
                if ( ! Modifier.isAbstract(caseClass.getModifiers()) && Modifier.isPublic( caseClass.getModifiers() ))
                {
                    cases.add( caseClass.newInstance() );
                }
            }
            catch ( Throwable e )
            {
                System.out.println("WARN: Unable to instantiate benchmark: " + caseClass.getName() + " (" + e + ")");
            }
        }


        return cases;
    }
}

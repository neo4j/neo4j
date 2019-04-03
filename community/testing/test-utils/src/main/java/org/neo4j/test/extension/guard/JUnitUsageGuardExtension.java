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
package org.neo4j.test.extension.guard;

import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.platform.commons.JUnitException;
import org.objectweb.asm.ClassReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static java.lang.String.format;
import static java.util.stream.Collectors.toSet;
import static org.objectweb.asm.ClassReader.SKIP_CODE;
import static org.objectweb.asm.ClassReader.SKIP_DEBUG;
import static org.objectweb.asm.ClassReader.SKIP_FRAMES;

public class JUnitUsageGuardExtension implements BeforeAllCallback
{
    @Override
    public void beforeAll( ExtensionContext context ) throws Exception
    {
        Class<?> testClazz = context.getRequiredTestClass();
        Set<String> testClasses = collectUsedTestClasses( testClazz );

        // we do not want to check platform or model classes so far
        testClasses.removeIf( s -> s.startsWith( "org.junit.platform" ) );
        testClasses.removeIf( s -> s.startsWith( "org.junit.runners.model" ) );

        Set<String> newJunitClasses = testClasses.stream().filter( s -> s.startsWith( "org.junit.jupiter" ) ).collect( toSet() );
        if ( newJunitClasses.isEmpty() || noOldJunitUsages( testClasses, newJunitClasses ) )
        {
            return;
        }
        // now testClasses should contain only old junit classes.
        testClasses.removeAll( newJunitClasses );
        throw new JUnitException( format( "Detect usage of classes from multiple junit versions in the single test class: %s.%n" +
                "Detected JUnit 5 classes: %s.%n" +
                "Detected Junit 4 classes: %s.", testClazz.getName(), sortedClasses( newJunitClasses ), sortedClasses( testClasses ) ) );
    }

    private static List<String> sortedClasses( Set<String> newJunitClasses )
    {
        ArrayList<String> strings = new ArrayList<>( newJunitClasses );
        Collections.sort( strings );
        return strings;
    }

    private static boolean noOldJunitUsages( Set<String> testClasses, Set<String> newJunitClasses )
    {
        return newJunitClasses.size() == testClasses.size();
    }

    private static Set<String> collectUsedTestClasses( Class<?> clazz ) throws IOException
    {
        ClassReader classReader = new ClassReader( clazz.getName() );
        DependenciesCollector dependenciesCollector = new DependenciesCollector();
        classReader.accept( dependenciesCollector, SKIP_DEBUG & SKIP_CODE & SKIP_FRAMES );
        return dependenciesCollector.getJunitTestClasses();
    }
}

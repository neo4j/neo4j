/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.backup.impl;

import org.junit.Test;

import java.util.Collection;
import java.util.function.Predicate;

import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.neo4j.backup.impl.BackupSupportingClassesFactoryProvider.getProvidersByPriority;

public class BackupSupportingClassesFactoryProviderTest
{
    @Test
    public void canLoadDefaultSupportingClassesFactory()
    {
        assertEquals( 1, findInstancesOf( BackupSupportingClassesFactoryProvider.class,
                allAvailableSupportingClassesFactories() ).size() );
        assertEquals( 2, allAvailableSupportingClassesFactories().size() );
    }

    @Test
    public void testDefaultModuleIsPrioritisedOverDummyModule()
    {
        assertEquals( BackupSupportingClassesFactoryProvider.class,
                getProvidersByPriority().findFirst().get().getClass() );
    }

    public static Collection<BackupSupportingClassesFactoryProvider> allAvailableSupportingClassesFactories()
    {
        return getProvidersByPriority().collect( toList() );
    }

    public static <DESIRED extends BackupSupportingClassesFactoryProvider> Collection<DESIRED> findInstancesOf(
            Class<DESIRED> desiredClass, Collection<? extends BackupSupportingClassesFactoryProvider> collection )
    {
        return collection
                .stream()
                .filter( isOfClass( desiredClass ) )
                .map( i -> (DESIRED) i )
                .collect( toList() );
    }

    private static Predicate<BackupSupportingClassesFactoryProvider> isOfClass(
            Class<? extends BackupSupportingClassesFactoryProvider> desiredClass )
    {
        return factory -> desiredClass.equals( factory.getClass() );
    }
}

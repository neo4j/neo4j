/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.backup;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class BackupSupportingClassesFactoryProviderTest
{

    @Test
    public void canLoadCommunitySupportingClassesFactory()
    {
        assertEquals( 1, findInstancesOf( CommunityBackupSupportingClassesFactoryProvider.class, allAvailableSupportingClassesFactories() ).size() );
        assertEquals( 2, allAvailableSupportingClassesFactories().size() );
    }

    @Test
    public void testCommunityModuleIsPrioritisedOverSslModule()
    {
        assertEquals( CommunityBackupSupportingClassesFactoryProvider.class, BackupSupportingClassesFactoryProvider.findBestProvider().get().getClass() );
    }

    public static Collection<BackupSupportingClassesFactoryProvider> allAvailableSupportingClassesFactories()
    {
        Collection<BackupSupportingClassesFactoryProvider> discovered = new ArrayList<>();
        BackupSupportingClassesFactoryProvider.load( BackupSupportingClassesFactoryProvider.class ).forEach( discovered::add );
        return discovered;
    }

    public static <DESIRED extends BackupSupportingClassesFactoryProvider> Collection<DESIRED> findInstancesOf( Class<DESIRED> desiredClass,
            Collection<? extends BackupSupportingClassesFactoryProvider> collection )
    {
        return collection
                .stream()
                .filter( isOfClass( desiredClass ) )
                .map( i -> (DESIRED) i )
                .collect( Collectors.toList() );
    }

    private static Predicate<BackupSupportingClassesFactoryProvider> isOfClass( Class<? extends BackupSupportingClassesFactoryProvider> desiredClass )
    {
        return factory -> desiredClass.equals( factory.getClass() );
    }
}

/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.backup.impl;

import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.neo4j.helpers.Service;

import static java.util.Comparator.comparingInt;

public class BackupSupportingClassesFactoryProvider extends Service
{
    /**
     * Create a new instance of a service implementation identified with the
     * specified key(s).
     *
     * @param key the main key for identifying this service implementation
     */
    public BackupSupportingClassesFactoryProvider( String key )
    {
        super( key );
    }

    public BackupSupportingClassesFactoryProvider()
    {
        super( "default-backup-support-provider" );
    }

    public BackupSupportingClassesFactory getFactory( BackupModule backupModule )
    {
        return new BackupSupportingClassesFactory( backupModule );
    }

    public static Stream<BackupSupportingClassesFactoryProvider> getProvidersByPriority()
    {
        Stream<BackupSupportingClassesFactoryProvider> providers = StreamSupport.stream(
                load( BackupSupportingClassesFactoryProvider.class ).spliterator(), false );
        // Inject the default provider into the stream, so it participates in sorting by priority.
        providers = Stream.concat( providers, Stream.of( new BackupSupportingClassesFactoryProvider() ) );
        return providers.sorted( comparingInt( BackupSupportingClassesFactoryProvider::getPriority ).reversed() );
    }

    /**
     * The higher the priority value, the greater the preference
     */
    protected int getPriority()
    {
        return 42;
    }
}

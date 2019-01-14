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

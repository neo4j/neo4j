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
package org.neo4j.graphdb.factory.module.id;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.function.Function;
import java.util.function.LongSupplier;

import org.neo4j.internal.id.BufferedIdController;
import org.neo4j.internal.id.BufferingIdGeneratorFactory;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.id.IdType;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.kernel.impl.api.KernelTransactionsSnapshot;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@PageCacheExtension
class IdContextFactoryBuilderTest
{
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private DefaultFileSystemAbstraction fs;
    @Inject
    private PageCache pageCache;
    private final JobScheduler jobScheduler = mock( JobScheduler.class );
    private final DatabaseIdRepository databaseIdRepository = new TestDatabaseIdRepository();

    @Test
    void requireFileSystemWhenIdGeneratorFactoryNotProvided()
    {
        NullPointerException exception = assertThrows( NullPointerException.class, () -> IdContextFactoryBuilder.of( jobScheduler ).build() );
        assertThat( exception.getMessage(), containsString( "File system is required" ) );
    }

    @Test
    void createContextWithCustomIdGeneratorFactoryWhenProvided()
    {
        IdGeneratorFactory idGeneratorFactory = mock( IdGeneratorFactory.class );
        IdContextFactory contextFactory = IdContextFactoryBuilder.of( fs, jobScheduler ).withIdGenerationFactoryProvider( any -> idGeneratorFactory ).build();
        DatabaseIdContext idContext = contextFactory.createIdContext( databaseIdRepository.get( "database" ).get() );

        IdGeneratorFactory bufferedGeneratorFactory = idContext.getIdGeneratorFactory();
        assertThat( idContext.getIdController(), instanceOf( BufferedIdController.class ) );
        assertThat( bufferedGeneratorFactory, instanceOf( BufferingIdGeneratorFactory.class ) );

        ((BufferingIdGeneratorFactory)bufferedGeneratorFactory).initialize( () -> mock( KernelTransactionsSnapshot.class ) );
        File file = testDirectory.file( "a" );
        IdType idType = IdType.NODE;
        LongSupplier highIdSupplier = () -> 0;
        int maxId = 100;

        idGeneratorFactory.open( pageCache, file, idType, highIdSupplier, maxId );

        verify( idGeneratorFactory ).open( pageCache, file, idType, highIdSupplier, maxId );
    }

    @Test
    void createContextWithFactoryWrapper()
    {
        IdGeneratorFactory idGeneratorFactory = mock( IdGeneratorFactory.class );
        Function<IdGeneratorFactory,IdGeneratorFactory> factoryWrapper = ignored -> idGeneratorFactory;

        IdContextFactory contextFactory = IdContextFactoryBuilder.of( fs, jobScheduler )
                                        .withFactoryWrapper( factoryWrapper )
                                        .build();

        DatabaseIdContext idContext = contextFactory.createIdContext( databaseIdRepository.get( "database" ).get() );

        assertSame( idGeneratorFactory, idContext.getIdGeneratorFactory() );
    }
}

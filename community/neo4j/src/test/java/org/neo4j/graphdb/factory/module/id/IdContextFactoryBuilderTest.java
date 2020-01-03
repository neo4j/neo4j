/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.util.function.Function;
import java.util.function.LongSupplier;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.kernel.impl.api.KernelTransactionsSnapshot;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.id.BufferedIdController;
import org.neo4j.kernel.impl.store.id.BufferingIdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdGenerator;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdGeneratorImpl;
import org.neo4j.kernel.impl.store.id.IdReuseEligibility;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.kernel.impl.store.id.configuration.CommunityIdTypeConfigurationProvider;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.test.extension.DefaultFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith( {DefaultFileSystemExtension.class, TestDirectoryExtension.class} )
class IdContextFactoryBuilderTest
{
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private DefaultFileSystemAbstraction fs;
    private final JobScheduler jobScheduler = mock( JobScheduler.class );

    @Test
    void createCommunityBufferedContextByDefault()
    {
        IdContextFactory idContextFactory = IdContextFactoryBuilder.of( fs, jobScheduler ).build();
        DatabaseIdContext idContext = idContextFactory.createIdContext( "database" );

        IdGeneratorFactory idGeneratorFactory = idContext.getIdGeneratorFactory();
        assertThat( idContext.getIdController(), instanceOf( BufferedIdController.class ) );
        assertThat( idGeneratorFactory, instanceOf( BufferingIdGeneratorFactory.class ) );

        ((BufferingIdGeneratorFactory)idGeneratorFactory).initialize( () -> mock( KernelTransactionsSnapshot.class ) );
        idGeneratorFactory.open( testDirectory.file( "a"), IdType.NODE, () -> 0, 100 ).close();
        idGeneratorFactory.open( testDirectory.file( "b"), IdType.PROPERTY, () -> 0, 100 ).close();

        BufferingIdGeneratorFactory bufferedFactory = (BufferingIdGeneratorFactory) idGeneratorFactory;
        assertThat( bufferedFactory.get( IdType.NODE ), instanceOf( IdGeneratorImpl.class ) );
        assertThat( bufferedFactory.get( IdType.PROPERTY ), not( instanceOf( IdGeneratorImpl.class ) ) );
    }

    @Test
    void requireFileSystemWhenIdGeneratorFactoryNotProvided()
    {
        NullPointerException exception = assertThrows( NullPointerException.class,
                () -> IdContextFactoryBuilder.of( new CommunityIdTypeConfigurationProvider(), jobScheduler ).build() );
        assertThat( exception.getMessage(), containsString( "File system is required" ) );
    }

    @Test
    void createContextWithCustomIdGeneratorFactoryWhenProvided()
    {
        IdGeneratorFactory idGeneratorFactory = mock( IdGeneratorFactory.class );
        IdContextFactory contextFactory = IdContextFactoryBuilder.of( fs, jobScheduler ).withIdGenerationFactoryProvider( any -> idGeneratorFactory ).build();
        DatabaseIdContext idContext = contextFactory.createIdContext( "database" );

        IdGeneratorFactory bufferedGeneratorFactory = idContext.getIdGeneratorFactory();
        assertThat( idContext.getIdController(), instanceOf( BufferedIdController.class ) );
        assertThat( bufferedGeneratorFactory, instanceOf( BufferingIdGeneratorFactory.class ) );

        ((BufferingIdGeneratorFactory)bufferedGeneratorFactory).initialize( () -> mock( KernelTransactionsSnapshot.class ) );
        File file = testDirectory.file( "a" );
        IdType idType = IdType.NODE;
        LongSupplier highIdSupplier = () -> 0;
        int maxId = 100;

        idGeneratorFactory.open( file, idType, highIdSupplier, maxId );

        verify( idGeneratorFactory ).open( file, idType, highIdSupplier, maxId );
    }

    @Test
    void createContextWithProvidedReusabilityCheck()
    {
        IdReuseEligibility reuseEligibility = mock( IdReuseEligibility.class );
        IdContextFactory contextFactory = IdContextFactoryBuilder.of( fs, jobScheduler ).withIdReuseEligibility( reuseEligibility ).build();
        DatabaseIdContext idContext = contextFactory.createIdContext( "database" );
        IdGeneratorFactory bufferedGeneratorFactory = idContext.getIdGeneratorFactory();

        assertThat( bufferedGeneratorFactory, instanceOf( BufferingIdGeneratorFactory.class ) );
        BufferingIdGeneratorFactory bufferedFactory = (BufferingIdGeneratorFactory) bufferedGeneratorFactory;

        KernelTransactionsSnapshot snapshot = mock( KernelTransactionsSnapshot.class );
        when( snapshot.allClosed() ).thenReturn( true );

        bufferedFactory.initialize( () -> snapshot );
        try ( IdGenerator idGenerator = bufferedFactory.open( testDirectory.file( "a" ), IdType.PROPERTY, () -> 100, 100 ) )
        {
            idGenerator.freeId( 15 );

            bufferedFactory.maintenance();
            verify( reuseEligibility ).isEligible( snapshot );
        }
    }

    @Test
    void createContextWithFactoryWrapper()
    {
        Function<IdGeneratorFactory,IdGeneratorFactory> factoryWrapper = mock( Function.class );
        IdGeneratorFactory idGeneratorFactory = mock( IdGeneratorFactory.class );
        when( factoryWrapper.apply( any() ) ).thenReturn( idGeneratorFactory );

        IdContextFactory contextFactory = IdContextFactoryBuilder.of( fs, jobScheduler )
                                        .withFactoryWrapper( factoryWrapper )
                                        .build();

        DatabaseIdContext idContext = contextFactory.createIdContext( "database" );

        assertSame( idGeneratorFactory, idContext.getIdGeneratorFactory() );
    }
}

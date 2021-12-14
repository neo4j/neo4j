package org.neo4j.kernel.database;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atMostOnce;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MapCachingDatabaseReferenceRepositoryTest
{
    private final DatabaseReferenceRepository delegate = Mockito.mock( DatabaseReferenceRepository.class );
    private final NamedDatabaseId dbId = DatabaseIdFactory.from( "random", UUID.randomUUID() );
    private final NormalizedDatabaseName name = new NormalizedDatabaseName( dbId.name() );
    private final NormalizedDatabaseName aliasName = new NormalizedDatabaseName( "foo" );
    private final DatabaseReference ref = new DatabaseReference.Internal( name, dbId );
    private final DatabaseReference aliasRef = new DatabaseReference.Internal( aliasName, dbId );

    private DatabaseReferenceRepository.Caching databaseRefRepo;

    @BeforeEach
    void setUp()
    {
        when( delegate.getByName( aliasName ) ).thenReturn( Optional.of( aliasRef ) );
        when( delegate.getByName( name ) ).thenReturn( Optional.of( ref ) );
        databaseRefRepo = new MapCachingDatabaseReferenceRepository( delegate );
    }

    @Test
    void shouldLookupByName()
    {
        var lookup = databaseRefRepo.getByName( name );
        var lookupAlias = databaseRefRepo.getByName( aliasName );
        var lookupUnknown = databaseRefRepo.getByName( new NormalizedDatabaseName( "unknown" ) );

        assertThat( lookup ).contains( ref );
        assertThat( lookupAlias ).contains( aliasRef );
        assertThat( lookupUnknown ).isEmpty();
    }

    @Test
    void shouldCacheByByName()
    {
        var lookup = databaseRefRepo.getByName( name );
        var lookup2 = databaseRefRepo.getByName( name );

        assertThat( lookup ).contains( ref );
        assertThat( lookup ).isEqualTo( lookup2 );

        verify( delegate, atMostOnce() ).getByName( name );
    }

    @Test
    void shouldNotCacheGetAllLookups()
    {
        databaseRefRepo.getAllDatabaseReferences();
        databaseRefRepo.getInternalDatabaseReferences();
        databaseRefRepo.getExternalDatabaseReferences();
        databaseRefRepo.getAllDatabaseReferences();
        databaseRefRepo.getInternalDatabaseReferences();
        databaseRefRepo.getExternalDatabaseReferences();

        verify( delegate, atLeast( 2 ) ).getAllDatabaseReferences();
        verify( delegate, atLeast( 2 ) ).getInternalDatabaseReferences();
        verify( delegate, atLeast( 2 ) ).getExternalDatabaseReferences();
    }
}

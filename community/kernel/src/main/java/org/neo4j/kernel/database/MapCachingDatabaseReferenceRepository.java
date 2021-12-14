package org.neo4j.kernel.database;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class MapCachingDatabaseReferenceRepository implements DatabaseReferenceRepository.Caching
{
    private final DatabaseReferenceRepository delegate;
    private volatile Map<NormalizedDatabaseName,DatabaseReference> databaseRefsByName;

    public MapCachingDatabaseReferenceRepository( DatabaseReferenceRepository delegate )
    {
        this.databaseRefsByName = new ConcurrentHashMap<>();
        this.delegate = delegate;
    }

    @Override
    public Optional<DatabaseReference> getByName( NormalizedDatabaseName databaseName )
    {
        return Optional.ofNullable( databaseRefsByName.computeIfAbsent( databaseName, this::lookupReferenceOnDelegate ) );
    }

    /**
     * May return null, as {@link ConcurrentHashMap#computeIfAbsent} uses null as a signal not to add an entry to for the given key.
     */
    private DatabaseReference lookupReferenceOnDelegate( NormalizedDatabaseName databaseName )
    {
        return delegate.getByName( databaseName ).orElse( null );
    }

    @Override
    public Set<DatabaseReference> getAllDatabaseReferences()
    {
        // Can't cache getAll call
        return delegate.getAllDatabaseReferences();
    }

    @Override
    public Set<DatabaseReference.Internal> getInternalDatabaseReferences()
    {
        // Can't cache getAll call
        return delegate.getInternalDatabaseReferences();
    }

    @Override
    public Set<DatabaseReference.External> getExternalDatabaseReferences()
    {
        // Can't cache getAll call
        return delegate.getExternalDatabaseReferences();
    }

    @Override
    public void invalidateAll()
    {
        this.databaseRefsByName = new ConcurrentHashMap<>();
    }
}

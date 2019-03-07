package org.neo4j.availability;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.availability.AvailabilityGuard;
import org.neo4j.kernel.availability.AvailabilityRequirement;
import org.neo4j.kernel.availability.CompositeDatabaseAvailabilityGuard;
import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.default_database;

@ExtendWith( TestDirectoryExtension.class )
class DatabaseAvailabilityIT
{
    @Inject
    private TestDirectory testDirectory;
    private GraphDatabaseAPI database;

    @BeforeEach
    void setUp()
    {
        database = (GraphDatabaseAPI) new GraphDatabaseFactory().newEmbeddedDatabase( testDirectory.storeDir() );
    }

    @AfterEach
    void tearDown()
    {
        database.shutdown();
    }

    @Test
    void anyOfDatabaseUnavailabilityIsGlobalUnavailability()
    {
        AvailabilityRequirement outerSpaceRequirement = () -> "outer space";
        DependencyResolver dependencyResolver = database.getDependencyResolver();
        DatabaseManager databaseManager = dependencyResolver.resolveDependency( DatabaseManager.class );
        Config config = dependencyResolver.resolveDependency( Config.class );
        CompositeDatabaseAvailabilityGuard compositeGuard = dependencyResolver.resolveDependency( CompositeDatabaseAvailabilityGuard.class );
        assertTrue( compositeGuard.isAvailable() );

        DatabaseContext systemContext = databaseManager.getDatabaseContext( SYSTEM_DATABASE_NAME ).get();
        DatabaseContext defaultContext = databaseManager.getDatabaseContext( config.get( default_database ) ).get();

        AvailabilityGuard systemGuard = systemContext.getDependencies().resolveDependency( DatabaseAvailabilityGuard.class );
        systemGuard.require( outerSpaceRequirement );
        assertFalse( compositeGuard.isAvailable() );

        systemGuard.fulfill( outerSpaceRequirement );
        assertTrue( compositeGuard.isAvailable() );

        AvailabilityGuard defaultGuard = defaultContext.getDependencies().resolveDependency( DatabaseAvailabilityGuard.class );
        defaultGuard.require( outerSpaceRequirement );
        assertFalse( compositeGuard.isAvailable() );

        defaultGuard.fulfill( outerSpaceRequirement );
        assertTrue( compositeGuard.isAvailable() );
    }
}

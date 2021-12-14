package org.neo4j.dbms.systemgraph;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import org.neo4j.configuration.helpers.RemoteUri;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.database.CommunityTopologyGraphDbmsModel;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.database.DatabaseReference;
import org.neo4j.kernel.database.NormalizedDatabaseName;

import static org.assertj.core.api.Assertions.assertThat;

public class CommunityTopologyGraphDbmsModelTest extends BaseTopologyGraphDbmsModelTest
{
    private CommunityTopologyGraphDbmsModel dbmsModel;

    @Override
    protected void createModel( Transaction tx )
    {
        dbmsModel = new CommunityTopologyGraphDbmsModel( tx );
    }

    @Test
    void canReturnAllInternalDatabaseReferences()
    {
        // given
        var fooDb = newDatabase( b -> b.withDatabase( "foo" ) );
        var barDb = newDatabase( b -> b.withDatabase( "bar" ) );
        createLocalAliasForDatabase( tx, "fooAlias", false, fooDb  );
        createLocalAliasForDatabase( tx, "fooOtherAlias", false, fooDb  );
        createLocalAliasForDatabase( tx, "barAlias", false, barDb );

        var expected = Set.of(
                new DatabaseReference.Internal( new NormalizedDatabaseName( "foo" ), fooDb ),
                new DatabaseReference.Internal( new NormalizedDatabaseName( "bar" ), barDb ),
                new DatabaseReference.Internal( new NormalizedDatabaseName( "fooAlias" ), fooDb ),
                new DatabaseReference.Internal( new NormalizedDatabaseName( "fooOtherAlias" ), fooDb ),
                new DatabaseReference.Internal( new NormalizedDatabaseName( "barAlias" ), barDb ) );

        // when
        var aliases = dbmsModel.getAllInternalDatabaseReferences();

        // then
        assertThat( aliases ).isEqualTo( expected );
    }

    @Test
    void canReturnAllExternalDatabaseReferences()
    {

        // given
        var remoteAddress = new SocketAddress( "my.neo4j.com", 7687 );
        var remoteNeo4j = new RemoteUri( "neo4j", List.of( remoteAddress ), null );
        createRemoteAliasForDatabase( tx, "fooAlias", "foo", remoteNeo4j );
        createRemoteAliasForDatabase( tx, "fooOtherAlias", "foo", remoteNeo4j );
        createRemoteAliasForDatabase( tx, "barAlias", "bar", remoteNeo4j );

        var expected = Set.of(
                new DatabaseReference.External( new NormalizedDatabaseName( "foo" ), new NormalizedDatabaseName( "fooAlias" ), remoteNeo4j ),
                new DatabaseReference.External( new NormalizedDatabaseName( "foo" ), new NormalizedDatabaseName( "fooOtherAlias" ), remoteNeo4j ),
                new DatabaseReference.External( new NormalizedDatabaseName( "bar" ), new NormalizedDatabaseName( "barAlias" ), remoteNeo4j ) );

        // when
        var aliases = dbmsModel.getAllExternalDatabaseReferences();

        // then
        assertThat( aliases ).isEqualTo( expected );
    }

    @Test
    void canReturnAllDatabaseReferences()
    {

        // given
        var fooDb = newDatabase( b -> b.withDatabase( "foo" ) );
        createLocalAliasForDatabase( tx, "fooAlias", false, fooDb  );
        var remoteAddress = new SocketAddress( "my.neo4j.com", 7687 );
        var remoteNeo4j = new RemoteUri( "neo4j", List.of( remoteAddress ), null );
        createRemoteAliasForDatabase( tx, "bar", "foo", remoteNeo4j );

        var expected = Set.of(
                new DatabaseReference.External( new NormalizedDatabaseName( "foo" ), new NormalizedDatabaseName( "bar" ), remoteNeo4j ),
                new DatabaseReference.Internal( new NormalizedDatabaseName( "foo" ), fooDb ),
                new DatabaseReference.Internal( new NormalizedDatabaseName( "fooAlias" ), fooDb ) );

        // when
        var aliases = dbmsModel.getAllDatabaseReferences();

        // then
        assertThat( aliases ).isEqualTo( expected );
    }
}

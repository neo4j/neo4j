package org.neo4j.internal.recordstorage;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.TestInstance;

import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.configuration.Config;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.EphemeralPageCacheExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.token.DelegatingTokenHolder;
import org.neo4j.token.TokenCreator;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.TokenHolder;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;

@TestInstance( TestInstance.Lifecycle.PER_CLASS )
@EphemeralPageCacheExtension
class SchemaStorageReadAndWriteTest
{
    @Inject
    private PageCache pageCache;
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private EphemeralFileSystemAbstraction fs;

    private RandomSchema randomSchema = new RandomSchema()
    {
        @Override
        public int nextRuleId()
        {
            return (int) storage.newRuleId();
        }
    };

    private SchemaStorage storage;
    private NeoStores neoStores;

    @BeforeAll
    void before() throws Exception
    {
        testDirectory.prepareDirectory( getClass(), "test" );
        var storeFactory = new StoreFactory( testDirectory.databaseLayout(), Config.defaults(), new DefaultIdGeneratorFactory( fs, immediate() ),
                pageCache, fs, NullLogProvider.getInstance() );
        neoStores = storeFactory.openNeoStores( true, StoreType.SCHEMA, StoreType.PROPERTY_KEY_TOKEN, StoreType.LABEL_TOKEN,
                StoreType.RELATIONSHIP_TYPE_TOKEN );
        AtomicInteger tokenIdCounter = new AtomicInteger();
        TokenCreator tokenCreator = (name, internal) -> tokenIdCounter.incrementAndGet();
        TokenHolders tokens = new TokenHolders( new DelegatingTokenHolder( tokenCreator, TokenHolder.TYPE_PROPERTY_KEY ),
                new DelegatingTokenHolder( tokenCreator, TokenHolder.TYPE_LABEL ),
                new DelegatingTokenHolder( tokenCreator, TokenHolder.TYPE_RELATIONSHIP_TYPE ) );
        tokens.setInitialTokens( StoreTokens.allTokens( neoStores ) );
        tokenIdCounter.set( Math.max( tokenIdCounter.get(), tokens.propertyKeyTokens().size() ) );
        tokenIdCounter.set( Math.max( tokenIdCounter.get(), tokens.labelTokens().size() ) );
        tokenIdCounter.set( Math.max( tokenIdCounter.get(), tokens.relationshipTypeTokens().size() ) );
        storage = new SchemaStorage( neoStores.getSchemaStore(), tokens );
    }

    @AfterAll
    void after()
    {
        neoStores.close();
    }

    @RepeatedTest( 2000 )
    void shouldPerfectlyPreserveSchemaRules() throws Exception
    {
        SchemaRule schemaRule = randomSchema.nextSchemaRule();
        storage.writeSchemaRule( schemaRule );
        SchemaRule returnedRule = storage.loadSingleSchemaRule( schemaRule.getId() );
        assertTrue( RandomSchema.schemaDeepEquals( returnedRule, schemaRule ),
                () -> "\n" + returnedRule + "\nwas not equal to\n" + schemaRule );
    }
}

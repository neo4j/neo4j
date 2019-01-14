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
package org.neo4j.kernel.impl.store;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.util.Arrays;
import java.util.Collection;
import java.util.stream.IntStream;

import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.internal.kernel.api.schema.IndexProviderDescriptor;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.kernel.api.schema.constraints.ConstraintDescriptorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.id.DefaultIdGeneratorFactory;
import org.neo4j.kernel.impl.store.record.ConstraintRule;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.SchemaRuleSerialization;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.storageengine.api.EntityType;
import org.neo4j.storageengine.api.schema.IndexDescriptorFactory;
import org.neo4j.storageengine.api.schema.SchemaRule;
import org.neo4j.storageengine.api.schema.StoreIndexDescriptor;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static java.nio.ByteBuffer.wrap;
import static org.junit.Assert.assertEquals;
import static org.neo4j.helpers.collection.Iterators.asCollection;
import static org.neo4j.kernel.api.schema.SchemaDescriptorFactory.forLabel;
import static org.neo4j.kernel.api.schema.SchemaDescriptorFactory.multiToken;
import static org.neo4j.kernel.impl.api.index.TestIndexProviderDescriptor.PROVIDER_DESCRIPTOR;
import static org.neo4j.storageengine.api.schema.IndexDescriptorFactory.forSchema;

public class SchemaStoreTest
{
    @ClassRule
    public static final PageCacheRule pageCacheRule = new PageCacheRule();
    private final EphemeralFileSystemRule fs = new EphemeralFileSystemRule();
    private final TestDirectory testDirectory = TestDirectory.testDirectory( fs );
    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule( fs ).around( testDirectory );

    private Config config;
    private SchemaStore store;
    private NeoStores neoStores;
    private StoreFactory storeFactory;

    @Before
    public void before()
    {
        config = Config.defaults();
        DefaultIdGeneratorFactory idGeneratorFactory = new DefaultIdGeneratorFactory( fs.get() );
        storeFactory = new StoreFactory( testDirectory.databaseLayout(), config, idGeneratorFactory, pageCacheRule.getPageCache( fs.get() ),
                fs.get(), NullLogProvider.getInstance(), EmptyVersionContextSupplier.EMPTY );
        neoStores = storeFactory.openAllNeoStores( true );
        store = neoStores.getSchemaStore();
    }

    @After
    public void after()
    {
        neoStores.close();
    }

    @Test
    public void storeAndLoadSchemaRule() throws Exception
    {
        // GIVEN
        StoreIndexDescriptor indexRule = forSchema( forLabel( 1, 4 ), PROVIDER_DESCRIPTOR ).withId( store.nextId() );

        // WHEN
        StoreIndexDescriptor readIndexRule = (StoreIndexDescriptor) SchemaRuleSerialization.deserialize(
                indexRule.getId(), wrap( SchemaRuleSerialization.serialize( indexRule ) ) );

        // THEN
        assertEquals( indexRule.getId(), readIndexRule.getId() );
        assertEquals( indexRule.schema(), readIndexRule.schema() );
        assertEquals( indexRule, readIndexRule );
        assertEquals( indexRule.providerDescriptor(), readIndexRule.providerDescriptor() );
    }

    @Test
    public void storeAndLoadCompositeSchemaRule() throws Exception
    {
        // GIVEN
        int[] propertyIds = {4, 5, 6, 7};
        StoreIndexDescriptor indexRule = forSchema( forLabel( 2, propertyIds ), PROVIDER_DESCRIPTOR ).withId( store.nextId() );

        // WHEN
        StoreIndexDescriptor readIndexRule = (StoreIndexDescriptor) SchemaRuleSerialization.deserialize(
                indexRule.getId(), wrap( SchemaRuleSerialization.serialize( indexRule ) ) );

        // THEN
        assertEquals( indexRule.getId(), readIndexRule.getId() );
        assertEquals( indexRule.schema(), readIndexRule.schema() );
        assertEquals( indexRule, readIndexRule );
        assertEquals( indexRule.providerDescriptor(), readIndexRule.providerDescriptor() );
    }

    @Test
    public void storeAndLoadMultiTokenSchemaRule() throws Exception
    {
        // GIVEN
        int[] propertyIds = {4, 5, 6, 7};
        int[] entityTokens = {2, 3, 4};
        StoreIndexDescriptor indexRule =
                forSchema( multiToken( entityTokens, EntityType.RELATIONSHIP, propertyIds ), PROVIDER_DESCRIPTOR ).withId( store.nextId() );

        // WHEN
        StoreIndexDescriptor readIndexRule =
                (StoreIndexDescriptor) SchemaRuleSerialization.deserialize( indexRule.getId(), wrap( SchemaRuleSerialization.serialize( indexRule ) ) );

        // THEN
        assertEquals( indexRule.getId(), readIndexRule.getId() );
        assertEquals( indexRule.schema(), readIndexRule.schema() );
        assertEquals( indexRule, readIndexRule );
        assertEquals( indexRule.providerDescriptor(), readIndexRule.providerDescriptor() );
    }

    @Test
    public void storeAndLoadAnyTokenMultiTokenSchemaRule() throws Exception
    {
        // GIVEN
        int[] propertyIds = {4, 5, 6, 7};
        int[] entityTokens = {};
        StoreIndexDescriptor indexRule = forSchema( multiToken( entityTokens, EntityType.NODE, propertyIds ), PROVIDER_DESCRIPTOR ).withId( store.nextId() );

        // WHEN
        StoreIndexDescriptor readIndexRule =
                (StoreIndexDescriptor) SchemaRuleSerialization.deserialize( indexRule.getId(), wrap( SchemaRuleSerialization.serialize( indexRule ) ) );

        // THEN
        assertEquals( indexRule.getId(), readIndexRule.getId() );
        assertEquals( indexRule.schema(), readIndexRule.schema() );
        assertEquals( indexRule, readIndexRule );
        assertEquals( indexRule.providerDescriptor(), readIndexRule.providerDescriptor() );
    }

    @Test
    public void storeAndLoad_Big_CompositeSchemaRule() throws Exception
    {
        // GIVEN
        StoreIndexDescriptor indexRule = forSchema( forLabel( 2, IntStream.range( 1, 200 ).toArray() ), PROVIDER_DESCRIPTOR ).withId( store.nextId() );

        // WHEN
        StoreIndexDescriptor readIndexRule = (StoreIndexDescriptor) SchemaRuleSerialization.deserialize(
                indexRule.getId(), wrap( SchemaRuleSerialization.serialize( indexRule ) ) );

        // THEN
        assertEquals( indexRule.getId(), readIndexRule.getId() );
        assertEquals( indexRule.schema(), readIndexRule.schema() );
        assertEquals( indexRule, readIndexRule );
        assertEquals( indexRule.providerDescriptor(), readIndexRule.providerDescriptor() );
    }

    @Test
    public void storeAndLoad_Big_CompositeMultiTokenSchemaRule() throws Exception
    {
        // GIVEN
        StoreIndexDescriptor indexRule =
                forSchema( multiToken( IntStream.range( 1, 200 ).toArray(), EntityType.RELATIONSHIP, IntStream.range( 1, 200 ).toArray() ),
                        PROVIDER_DESCRIPTOR ).withId( store.nextId() );

        // WHEN
        StoreIndexDescriptor readIndexRule =
                (StoreIndexDescriptor) SchemaRuleSerialization.deserialize( indexRule.getId(), wrap( SchemaRuleSerialization.serialize( indexRule ) ) );

        // THEN
        assertEquals( indexRule.getId(), readIndexRule.getId() );
        assertEquals( indexRule.schema(), readIndexRule.schema() );
        assertEquals( indexRule, readIndexRule );
        assertEquals( indexRule.providerDescriptor(), readIndexRule.providerDescriptor() );
    }

    @Test
    public void storeAndLoadAllRules()
    {
        // GIVEN
        long indexId = store.nextId();
        long constraintId = store.nextId();
        Collection<SchemaRule> rules = Arrays.asList(
                uniqueIndexRule( indexId, constraintId, PROVIDER_DESCRIPTOR, 2, 5, 3 ),
                constraintUniqueRule( constraintId, indexId, 2, 5, 3 ),
                indexRule( store.nextId(), PROVIDER_DESCRIPTOR, 0, 5 ),
                indexRule( store.nextId(), PROVIDER_DESCRIPTOR, 1, 6, 10, 99 ),
                constraintExistsRule( store.nextId(), 5, 1 )
            );

        for ( SchemaRule rule : rules )
        {
            storeRule( rule );
        }

        // WHEN
        Collection<SchemaRule> readRules = asCollection( store.loadAllSchemaRules() );

        // THEN
        assertEquals( rules, readRules );
    }

    private long storeRule( SchemaRule rule )
    {
        Collection<DynamicRecord> records = store.allocateFrom( rule );
        for ( DynamicRecord record : records )
        {
            store.updateRecord( record );
        }
        return Iterables.first( records ).getId();
    }

    private StoreIndexDescriptor indexRule( long ruleId, IndexProviderDescriptor descriptor, int labelId, int... propertyIds )
    {
        return IndexDescriptorFactory.forSchema( forLabel( labelId, propertyIds ), descriptor ).withId( ruleId );
    }

    private StoreIndexDescriptor uniqueIndexRule( long ruleId, long owningConstraint, IndexProviderDescriptor descriptor, int labelId, int... propertyIds )
    {
        return IndexDescriptorFactory.uniqueForSchema( forLabel( labelId, propertyIds ), descriptor ).withIds( ruleId, owningConstraint );
    }

    private ConstraintRule constraintUniqueRule( long ruleId, long ownedIndexId, int labelId, int... propertyIds )
    {
        return ConstraintRule.constraintRule( ruleId, ConstraintDescriptorFactory.uniqueForLabel( labelId, propertyIds ), ownedIndexId );
    }

    private ConstraintRule constraintExistsRule( long ruleId, int labelId, int... propertyIds )
    {
        return ConstraintRule.constraintRule( ruleId, ConstraintDescriptorFactory.existsForLabel( labelId, propertyIds ) );
    }
}

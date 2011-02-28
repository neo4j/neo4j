package org.neo4j.kernel.impl.nioneo.store;

import static java.lang.Math.pow;
import static org.junit.Assert.fail;
import static org.neo4j.kernel.impl.AbstractNeo4jTestCase.deleteFileOrDirectory;

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;

@Ignore( "Until Johan have added those checks in RelationshipTypeStore" )
public class TestUpgradeStore
{
    private static final String PATH = "target/var/upgrade";
    
    @Before
    public void doBefore()
    {
        deleteFileOrDirectory( PATH );
    }
    
    @Test
    public void makeSureStoreWithTooManyRelationshipTypesCannotBeUpgraded() throws Exception
    {
        new EmbeddedGraphDatabase( PATH ).shutdown();
        createManyRelationshipTypes();
        decrementRelationshipTypeStoreVersion();
        try
        {
            new EmbeddedGraphDatabase( PATH );
            fail( "Shouldn't be able to upgrade with that many types set" );
        }
        catch ( IllegalStoreVersionException e )
        {   // Good
        }
    }
    
    private void decrementRelationshipTypeStoreVersion()
    {
        // TODO Auto-generated method stub
        
    }

    private void createManyRelationshipTypes()
    {
        String fileName = new File( PATH, "neostore.relationshiptypestore.db" ).getAbsolutePath();
        Map<Object, Object> config = MapUtil.<Object, Object>genericMap( IdGeneratorFactory.class, new NoLimitidGeneratorFactory() );
        RelationshipTypeStore store = new RelationshipTypeStoreWithOneOlderVersion( fileName, config, IdType.RELATIONSHIP_TYPE );
        int numberOfTypes = (int)pow( 2, 16 );
        for ( int i = 0; i < numberOfTypes; i++ )
        {
            String name = "type" + i;
            RelationshipTypeRecord record = new RelationshipTypeRecord( i );
            record.setCreated();
            record.setInUse( true );
            int typeBlockId = (int) store.nextBlockId();
            record.setTypeBlock( typeBlockId );
            int length = name.length();
            char[] chars = new char[length];
            name.getChars( 0, length, chars, 0 );
            Collection<DynamicRecord> typeRecords = store.allocateTypeNameRecords( typeBlockId, chars );
            for ( DynamicRecord typeRecord : typeRecords )
            {
                record.addTypeRecord( typeRecord );
            }
            store.setHighId( store.getHighId()+1 );
            store.updateRecord( record );
        }
        store.close();
    }
    
    private static class RelationshipTypeStoreWithOneOlderVersion extends RelationshipTypeStore
    {
        private boolean versionCalled;
        
        public RelationshipTypeStoreWithOneOlderVersion( String fileName, Map<?, ?> config,
                IdType idType )
        {
            super( fileName, config, idType );
        }
        
        @Override
        public String getTypeAndVersionDescriptor()
        {
            // This funky method will trick the store, telling it that it's the new version
            // when it loads (so that it validates OK). Then when closing it and writing
            // the version it will write the older version.
            if ( !versionCalled )
            {
                versionCalled = true;
                return super.getTypeAndVersionDescriptor();
            }
            else
            {
                // TODO This shouldn't be hard coded like this, boring to keep in sync
                // when version changes
                return "RelationshipTypeStore v0.9.5";
            }
        }
    }
    
    private static class NoLimitidGeneratorFactory implements IdGeneratorFactory
    {
        private final Map<IdType, IdGenerator> generators = new HashMap<IdType, IdGenerator>();
        
        public IdGenerator open( String fileName, int grabSize, IdType idType,
                long highestIdInUse )
        {
            IdGenerator generator = new IdGeneratorImpl( fileName, grabSize, Long.MAX_VALUE );
            generators.put( idType, generator );
            return generator;
        }
        
        public IdGenerator get( IdType idType )
        {
            return generators.get( idType );
        }
        
        public void create( String fileName )
        {
            IdGeneratorImpl.createGenerator( fileName );
        }
        
        public void updateIdGenerators( NeoStore neoStore )
        {
            neoStore.updateIdGenerators();
        }
    }
}

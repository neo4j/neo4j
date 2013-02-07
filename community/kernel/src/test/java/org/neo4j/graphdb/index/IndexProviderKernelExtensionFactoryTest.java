package org.neo4j.graphdb.index;

import java.util.Iterator;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.ListIndexIterable;
import org.neo4j.test.ImpermanentDatabaseRule;

public class IndexProviderKernelExtensionFactoryTest
{
    private boolean loaded = false;

    @Rule
    public ImpermanentDatabaseRule db = new ImpermanentDatabaseRule()
    {
        @Override
        protected void configure( GraphDatabaseFactory databaseFactory )
        {
            ListIndexIterable indexProviders = new ListIndexIterable();
            indexProviders.setIndexProviders( Iterables.toList(Iterables.<IndexProvider, IndexProvider>iterable( new TestIndexProvider() )) );
            databaseFactory.setIndexProviders( indexProviders );
        }
    };

    @Test
    public void testIndexProviderKernelExtensionFactory()
    {
        Assert.assertThat(loaded, CoreMatchers.equalTo( true ));
    }

    private class TestIndexProvider
        extends IndexProvider
    {
        private TestIndexProvider( )
        {
            super( "TEST");
        }

        @Override
        public IndexImplementation load( DependencyResolver dependencyResolver ) throws Exception
        {
            loaded = true;
            return null;
        }
    }
}

package org.neo4j.kernel.impl.index.schema;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.schema.index.TestIndexDescriptorFactory;
import org.neo4j.storageengine.api.schema.IndexDescriptor;
import org.neo4j.storageengine.api.schema.StoreIndexDescriptor;
import org.neo4j.values.storable.ValueGroup;

@RunWith( Parameterized.class )
public class NativeUniqueIndexPopulatorTest<KEY extends NativeIndexKey<KEY>, VALUE extends NativeIndexValue> extends NativeIndexPopulatorTests.Unique<KEY,VALUE>
{
    @Parameterized.Parameters(name = "{index}: {0}")
    public static Collection<Object[]> data()
    {
        IndexDescriptor descriptor = TestIndexDescriptorFactory.uniqueForLabel( 42, 666 );
        return Arrays.asList( new Object[][]{
                {"Number",
                        numberPopulatorFactory(),
                        (LayoutTestUtilFactory) () -> new UniqueLayoutTestUtil<>( new NumberLayoutTestUtil( descriptor ) )
                },
                {"String",
                        (PopulatorFactory) StringIndexPopulator::new,
                        (LayoutTestUtilFactory) () -> new UniqueLayoutTestUtil<>( new StringLayoutTestUtil( descriptor ) )
                },
                {"Date",
                        temporalPopulatorFactory( ValueGroup.DATE ),
                        (LayoutTestUtilFactory) () -> new UniqueLayoutTestUtil<>( new DateLayoutTestUtil( descriptor ) )
                },
                {"DateTime",
                        temporalPopulatorFactory( ValueGroup.ZONED_DATE_TIME ),
                        (LayoutTestUtilFactory) () -> new UniqueLayoutTestUtil<>( new DateTimeLayoutTestUtil( descriptor ) )
                },
                {"Duration",
                        temporalPopulatorFactory( ValueGroup.DURATION ),
                        (LayoutTestUtilFactory) () -> new UniqueLayoutTestUtil<>( new DurationLayoutTestUtil( descriptor ) )
                },
                {"LocalDateTime",
                        temporalPopulatorFactory( ValueGroup.LOCAL_DATE_TIME ),
                        (LayoutTestUtilFactory) () -> new UniqueLayoutTestUtil<>( new LocalDateTimeLayoutTestUtil( descriptor ) )
                },
                {"LocalTime",
                        temporalPopulatorFactory( ValueGroup.LOCAL_TIME ),
                        (LayoutTestUtilFactory) () -> new UniqueLayoutTestUtil<>( new LocalTimeLayoutTestUtil( descriptor ) )
                },
                {"LocalDateTime",
                        temporalPopulatorFactory( ValueGroup.LOCAL_DATE_TIME ),
                        (LayoutTestUtilFactory) () -> new UniqueLayoutTestUtil<>( new LocalDateTimeLayoutTestUtil( descriptor ) )
                },
                {"Time",
                        temporalPopulatorFactory( ValueGroup.ZONED_TIME ),
                        (LayoutTestUtilFactory) () -> new UniqueLayoutTestUtil<>( new TimeLayoutTestUtil( descriptor ) )
                },
                // todo { Spatial has it's own subclass because it need to override some of the test methods }
        } );
    }

    private final PopulatorFactory<KEY,VALUE> populatorFactory;
    private final LayoutTestUtilFactory<KEY,VALUE> layoutTestUtilFactory;

    @SuppressWarnings( "unused" )
    public NativeUniqueIndexPopulatorTest( String name, PopulatorFactory<KEY,VALUE> populatorFactory, LayoutTestUtilFactory<KEY,VALUE> layoutTestUtilFactory )
    {
        this.populatorFactory = populatorFactory;
        this.layoutTestUtilFactory = layoutTestUtilFactory;
    }

    @Override
    NativeIndexPopulator<KEY,VALUE> createPopulator() throws IOException
    {
        return populatorFactory.create( pageCache, fs, getIndexFile(), layout, monitor, indexDescriptor );
    }

    @Override
    LayoutTestUtil<KEY,VALUE> createLayoutTestUtil()
    {
        return layoutTestUtilFactory.create();
    }

    private static PopulatorFactory<NumberIndexKey,NativeIndexValue> numberPopulatorFactory()
    {
        return NumberIndexPopulator::new;
    }

    private static <TK extends NativeIndexSingleValueKey<TK>> PopulatorFactory<TK,NativeIndexValue> temporalPopulatorFactory( ValueGroup temporalValueGroup )
    {
        return ( pageCache, fs, storeFile, layout, monitor, descriptor ) ->
        {
            TemporalIndexFiles.FileLayout<TK> fileLayout = new TemporalIndexFiles.FileLayout<>( storeFile, layout, temporalValueGroup );
            return new TemporalIndexPopulator.PartPopulator<>( pageCache, fs, fileLayout, monitor, descriptor );
        };
    }

    @FunctionalInterface
    private interface PopulatorFactory<KEY extends NativeIndexKey<KEY>, VALUE extends NativeIndexValue>
    {
        NativeIndexPopulator<KEY,VALUE> create( PageCache pageCache, FileSystemAbstraction fs, File storeFile, IndexLayout<KEY,VALUE> layout,
                IndexProvider.Monitor monitor, StoreIndexDescriptor descriptor ) throws IOException;
    }
}

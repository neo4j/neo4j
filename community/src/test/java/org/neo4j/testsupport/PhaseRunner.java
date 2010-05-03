package org.neo4j.testsupport;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

import org.junit.runner.Description;
import org.junit.runner.Runner;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.ParentRunner;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.TestClass;

public class PhaseRunner extends ParentRunner<Runner>
{
    @Target( ElementType.METHOD )
    @Retention( RetentionPolicy.RUNTIME )
    public @interface Phase
    {
        int value() default 0;
    }

    private static final Comparator<FrameworkMethod> PHASE_COMPARATOR = new Comparator<FrameworkMethod>()
    {
        public int compare( FrameworkMethod o1, FrameworkMethod o2 )
        {
            Class<?> c1 = o1.getMethod().getDeclaringClass();
            Class<?> c2 = o2.getMethod().getDeclaringClass();
            if ( c1 == c2 )
            {
                return o1.getAnnotation( Phase.class ).value()
                       - o2.getAnnotation( Phase.class ).value();
            }
            else if ( c1.isAssignableFrom( c2 ) )
            {
                return -1;
            }
            else
            {
                return 1;
            }
        }
    };

    private List<Runner> runners;

    public PhaseRunner( Class<?> testClass ) throws InitializationError
    {
        super( testClass );
    }

    private static class InstanceRunner extends BlockJUnit4ClassRunner
    {
        private final Object instance;
        private final String name;

        InstanceRunner( Object instance, String name )
                                                      throws InitializationError
        {
            super( instance.getClass() );
            this.instance = instance;
            this.name = name;
        }

        @Override
        protected void validateConstructor( List<Throwable> errors )
        {
        }

        @Override
        protected Object createTest() throws Exception
        {
            return instance;
        }

        @Override
        protected Description describeChild( FrameworkMethod method )
        {
            Description child = super.describeChild( method );
            return Description.createTestDescription( child.getTestClass(),
                    name + ":" + child.getMethodName() );
        }
    }

    @Override
    protected void collectInitializationErrors( List<Throwable> errors )
    {
        super.collectInitializationErrors( errors );
        TestClass testclass = this.getTestClass();
        Object target = null;
        try
        {
            target = testclass.getOnlyConstructor().newInstance();
        }
        catch ( InvocationTargetException e )
        {
            errors.add( e.getTargetException() );
        }
        catch ( Throwable e )
        {
            errors.add( e );
        }
        List<FrameworkMethod> methods = testclass.getAnnotatedMethods( Phase.class );
        Collections.sort( methods, PHASE_COMPARATOR );
        runners = new LinkedList<Runner>();
        for ( FrameworkMethod method : methods )
        {
            validatePublicObjectNoArg( method.getMethod(), errors );
            if ( target != null )
            {
                Object instance;
                try
                {
                    instance = method.invokeExplosively( target );
                }
                catch ( Throwable e )
                {
                    errors.add( e );
                    continue;
                }
                try
                {
                    runners.add( new InstanceRunner( instance,
                            testclass.getJavaClass().getSimpleName() + "/"
                                    + method.getName() ) );
                }
                catch ( InitializationError e )
                {
                    for ( Throwable t : e.getCauses() )
                    {
                        t.printStackTrace();
                    }
                    errors.addAll( e.getCauses() );
                }
            }
        }
    }

    private void validatePublicObjectNoArg( Method fMethod,
            List<Throwable> errors )
    {
        if ( !Modifier.isPublic( fMethod.getDeclaringClass().getModifiers() ) )
            errors.add( new Exception( "Class "
                                       + fMethod.getDeclaringClass().getName()
                                       + " should be public" ) );
        if ( !Modifier.isPublic( fMethod.getModifiers() ) )
            errors.add( new Exception( "Method " + fMethod.getName()
                                       + "() should be public" ) );
        if ( fMethod.getReturnType() == Void.TYPE )
            errors.add( new Exception( "Method " + fMethod.getName()
                                       + "() should not be void" ) );
        if ( fMethod.getParameterTypes().length != 0 )
            errors.add( new Exception( "Method " + fMethod.getName()
                                       + " should have no parameters" ) );
    }

    @Override
    protected Description describeChild( Runner child )
    {
        return child.getDescription();
    }

    @Override
    protected List<Runner> getChildren()
    {
        return Collections.unmodifiableList( runners );
    }

    @Override
    protected void runChild( Runner child, RunNotifier notifier )
    {
        child.run( notifier );
    }
}

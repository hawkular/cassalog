package org.cassalog.core

import org.testng.annotations.Test

import static org.testng.Assert.assertEquals
import static org.testng.Assert.expectThrows

/**
 * @author jsanda
 */
class BootstrapTest extends CassalogBaseTest {

    @Test
    void applyChangesWithBootstrap() {
        keyspace = 'bootstrap_test'

        resetSchema(keyspace)

        def script = getClass().getResource('/bootstrap/bootstrap_test.groovy').toURI()

        CassalogImpl cassalog = new CassalogImpl(keyspace: keyspace, session: session)
        cassalog.execute(script, [keyspace: keyspace, session: session])
    }

    @Test
    void applyIncludedChangesWithBootstrap() {
        keyspace = 'bootstrap_test'

        resetSchema(keyspace)

        def script = getClass().getResource('/bootstrap/bootstrap_include.groovy').toURI()

        CassalogImpl cassalog = new CassalogImpl(keyspace: keyspace, session: session)
        cassalog.execute(script, [keyspace: keyspace, session: session])
    }

    @Test
    void loadChangeSetsWithBootstrap() {
        keyspace = 'bootstrap_test'

        resetSchema(keyspace)

        def script = getClass().getResource('/bootstrap/bootstrap_test.groovy').toURI()

        CassalogImpl cassalog = new CassalogImpl(keyspace: keyspace, session: session)

        assertEquals(cassalog.load(script, [], [keyspace: keyspace, session: session]).size(), 2)
    }

    @Test(expectedExceptions = BootstrapException)
    void bootstrapCanOnlyBeUsedBeforeSchemaChanges() {
        keyspace = 'bootstrap_test'

        resetSchema(keyspace)
        
        def script = getClass().getResource('/bootstrap/bootstrap_fail.groovy').toURI()

        CassalogImpl cassalog = new CassalogImpl(keyspace: keyspace, session: session)
        cassalog.execute(script, [keyspace: keyspace, session: session])
    }

}

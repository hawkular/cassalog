package org.cassalog.core

import org.testng.annotations.Test

import static org.testng.Assert.assertEquals

/**
 * @author jsanda
 */
class LoadChangeSetsTest extends CassalogBaseTest {

    @Test
    void loadChangeSets() {
        keyspace = 'tags_test'
        def script = getClass().getResource('/tags_test/script1.groovy').toURI()
        def cassalog = new CassalogImpl(keyspace: keyspace, session: session)
        def changeSets = cassalog.load(script, ['dev'], [keyspace: keyspace])

        assertEquals(changeSets.size(), 2)
        assertEquals(changeSets[0].version, 'table-1')
        assertEquals(changeSets[1].version, 'dev-data')
    }

}

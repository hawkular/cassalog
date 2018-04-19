package org.cassalog.core

/**
 * @author jsanda
 */
class BootstrapException extends RuntimeException {

    BootstrapException() {
        super()
    }

    BootstrapException(String message) {
        super(message)
    }

    BootstrapException(String message, Throwable cause) {
        super(message, cause)
    }
}

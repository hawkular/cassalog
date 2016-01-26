appender("file", FileAppender) {
  file = "target/test.log"
  append = false
  encoder(PatternLayoutEncoder) {
    pattern = "%-5level %date{ISO8601} [%thread] %class:%M:%L - %message%n"
  }
}

appender("console", ConsoleAppender) {
  encoder(PatternLayoutEncoder) {
    pattern = "%-5level %date{ISO8601} [%thread] %class:%M:%L - %message%n"
  }
}

root(WARN, ["console", "file"])
logger("org.cassalog", DEBUG, ["console", "file"], additivity = false)
logger("com.datastax", INFO, ["console", "file"], additivity = false)
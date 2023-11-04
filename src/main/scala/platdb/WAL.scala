package platdb

import scala.util.Try

// TODO: implement wal for meta info.
private[platdb] class Recover(val fileManager:FileManager):
    def recover():Try[Meta] = ???
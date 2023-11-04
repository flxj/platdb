/*
 * Copyright (C) 2023 flxj(https://github.com/flxj)
 *
 * All Rights Reserved.
 *
 * Use of this source code is governed by an Apache-style
 * license that can be found in the LICENSE file.
 */
package platdb

import scala.util.Try

// TODO: implement wal for meta info.
private[platdb] class Recover(val fileManager:FileManager):
    def recover():Try[Meta] = ???
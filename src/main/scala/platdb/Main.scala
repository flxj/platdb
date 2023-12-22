/*
   Copyright (C) 2023 flxj(https://github.com/flxj)

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package platdb

import java.io.File
import scala.util.{Failure,Success,Try}
import scala.concurrent.{Future,Await}
import com.typesafe.config.ConfigFactory

implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global

object PlatDB:
    @main def main(args: String*) =
        if args.length == 0 || args(0) == "" then
            throw new Exception("not found config file path parameter")
        
        val confPath = args(0)

        val config = ConfigFactory.parseFile(new File(confPath))
        //
        val dbPath = config.getString("database.path")
        if dbPath.length == 0 then
            throw new Exception("Illegal database.path parameter: data file cannot be empty")

        var dbTimeout = config.getInt("database.timeout")
        if dbTimeout < 0 then
            throw new Exception(s"Illegal database.timeout parameter ${dbTimeout}: the timeout cannot be negative")
        else if dbTimeout == 0 then
            dbTimeout = DB.defaultTimeout

        var dbBufSize = config.getInt("database.bufSize")
        if dbBufSize < 0 then
            throw new Exception(s"Illegal database.bufSize parameter ${dbBufSize}: the bufSize cannot be negative")
        else if dbBufSize == 0 then
            dbBufSize = DB.defaultBufSize
        
        var dbReadonly = config.getBoolean("database.readonly")
        var dbFillPercent = config.getDouble("database.fillPercent")
        if dbFillPercent < 0.0 || dbFillPercent > 1.0 then
            throw new Exception(s"Illegal database.fillPercent parameter ${dbFillPercent}: the fillPercent cannot be negative or lager than 1.0")
        else if dbFillPercent == 0.0 then
            dbFillPercent = DB.defaultFillPercent
        
        var dbTmpDir = config.getString("database.tmpDir")
        if dbTmpDir == "" then
            dbTmpDir = System.getProperty("java.io.tmpdir")

        val svcPort = config.getInt("service.port")
        if svcPort <= 0 then
            throw new Exception(s"Illegal service.port parameter ${svcPort}: the port cannot be negative")

        var svcHost = config.getString("service.host")
        if svcHost.length == 0 then
            throw new Exception(s"Illegal service.host parameter ${svcHost}")
        
        val svcLogLeval = config.getString("service.logLevel")

        val server = Server(ServerOptions(dbPath,Options(dbTimeout,dbBufSize,dbReadonly,dbFillPercent,dbTmpDir),svcHost,svcPort,svcLogLeval))
        server.run()

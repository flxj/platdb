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

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import scala.io.StdIn

//
case class ServerOptions(path:String,readonly:Boolean,bufSize:Int,timeout:Int)

class Server(val ops:ServerOptions):
    def run():Unit = 
        implicit val system = ActorSystem(Behaviors.empty, "platdb")
        // needed for the future flatMap/onComplete in the end
        implicit val executionContext = system.executionContext

        val route =
            path("hello") {
                get {
                    complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, "<h1>hello platdb</h1>"))
                }
        }

        val bindingFuture = Http().newServerAt("localhost", 8080).bind(route)

        println(s"Server now online. Please navigate to http://localhost:8080/hello\nPress RETURN to stop...")
        StdIn.readLine() // let it run until user presses return
        bindingFuture
            .flatMap(_.unbind()) // trigger unbinding from the port
            .onComplete(_ => system.terminate()) // and shutdown when done

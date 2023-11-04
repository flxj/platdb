/*
 * Copyright (C) 2023 flxj(https://github.com/flxj)
 *
 * All Rights Reserved.
 *
 * Use of this source code is governed by an Apache-style
 * license that can be found in the LICENSE file.
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

package com.tyntec.katrina.eventstore

import akka.actor.{ActorRef, Actor}
import akka.actor.Actor.Receive
import com.datastax.driver.core.Cluster
import com.google.common.util.concurrent.{ListenableFuture, Futures, FutureCallback}

import scala.concurrent.{Future, Promise}

/**
 * Created by kirillprasalov on 15.03.16.
 */
class PersistActor(cluster: Cluster, acker: ActorRef) extends Actor {

  val session = cluster.connect("akka-keyspace")

  val preparedStatement = session.prepare("INSERT INTO events(key, value) VALUES (?, ?);")

  def save(event: CassandraEvent): Unit =
    session.executeAsync(preparedStatement.bind(event.key, event.value)).asScalaFuture

  def receive: Receive = {
    case (k: String, v: String) => {
      val event = new CassandraEvent(k, v)
      acker ! Ack
      acker ! CanAccept(event)
    }
    case Accepted(event) => {
      save(event)
    }
  }

  implicit class ScalaFriendlyListenableFuture[T](lf: ListenableFuture[T]) {
    def asScalaFuture: Future[T] = {
      val p = Promise[T]()
      Futures.addCallback(lf, new FutureCallback[T] {
        def onFailure(t: Throwable): Unit = p failure t
        def onSuccess(result: T): Unit    = p success result
      })
      p.future
    }
  }
}

case class CassandraEvent(val key: String, val value: String)


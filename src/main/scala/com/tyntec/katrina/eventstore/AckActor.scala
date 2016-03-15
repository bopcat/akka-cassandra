package com.tyntec.katrina.eventstore

import akka.actor.Actor
import akka.actor.Actor.Receive

/**
 * Created by kirillprasalov on 15.03.16.
 */
class AckActor(val acker: Acker, val limit: Int) extends Actor {

  var events: Map[CassandraEvent, String] = Map()

  override def receive: Receive = {
    case CanAccept(event) => {
      if (events.size < limit)
        sender ! Accepted(event)
      else
        acker.fail(event.key, event.value)
    }
    case Ack => {
      val (done, notDone) = events.partition(_._2.equals(""))
      events = notDone
      for (event <- done) acker.ack(event._1.key, event._1.value)
    }
    case WaitFor(event) => events = events + (event -> "")
  }
}

class Acker {
  def ack(k: String, v: String): Unit = println("Acked " + k + "=>" + v)
  def fail(k: String, v: String): Unit = println("Failed " + k + "=>" + v)
}

case class CanAccept(val event: CassandraEvent)
case object Ack
case class WaitFor(val event: CassandraEvent)
case class Accepted(val event: CassandraEvent)
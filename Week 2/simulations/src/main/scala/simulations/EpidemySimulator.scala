package simulations

import math.random

class EpidemySimulator extends Simulator {

  def randomBelow(i: Int) = (random * i).toInt

  def probability(p: Double) = randomBelow(100) < p * 100

  protected[simulations] object SimConfig {
    val population: Int = 300
    val roomRows: Int = 8
    val roomColumns: Int = 8

    val prevalenceRate: Double = .01
    val transmissibilityRate: Double = .40
    val deathRate: Double = .25
    val takeAirplaneRate: Double = .01

    val maxDaysBeforeMove: Int = 5
    val daysToBecomeSick: Int = 6
    val daysToDie: Int = 14
    val daysToBecomeImmune: Int = 16
    val daysToTurnHealthy: Int = 18

    val withAirplane: Boolean = false
    val reduceMobility: Boolean = false
    val withVaccines: Boolean = false

    val mobilityReductionRate: Double = .5
    val infectedMobilityReductionRate: Double = .25
    val vaccinationRate: Double = .05

  }

  import SimConfig._

  val persons: List[Person] = {

    //Remove element at index `index` from the list
    def removeIndex(list: List[Person], index: Int) = {
      val (head, tail) = list.splitAt(index)
      head ::: tail.drop(1)
    }

    //Randomly create a new list of persons which conform to the predicate
    def randomize(list: List[Person], acc: List[Person], max: Int, predicate: Person => Boolean): List[Person] = {
      if(acc.size == max)
        acc
      else {
        val i = randomBelow(list.size)
        val p = list(i)
        if(predicate(p))
          randomize(removeIndex(list, i), acc :+ p, max, predicate)
        else
          randomize(list, acc, max, predicate)
      }
    }

    //Populate the initial list
    val list = (for(i <- 0 until population) yield new Person(i)).toList

    //Vaccinate some persons
    if(withVaccines)
      randomize(list, List(), (population * vaccinationRate).toInt, p => {
        val vaccinated = p.vaccinated
        p.vaccinated = true
        !vaccinated
      })

    //Infect some persons
    randomize(list, List(), (population * prevalenceRate).toInt, p => {
      val infected = p.infected
      p.infect
      !infected
    })

    list
  }

  def roomIsInfected(room: Pos): Boolean =
    persons.exists { p =>
      room == p.pos && p.isInfectious
    }

  def roomHasVisibleInfected(room: Pos): Boolean =
    persons.exists { p =>
      room == p.pos && p.isVisibleInfectious
    }

  class Person (val id: Int) {

    var infected = false
    var sick = false
    var immune = false
    var dead = false
    var vaccinated = false

    var row = randomBelow(roomRows)
    var col = randomBelow(roomColumns)

    def pos = Pos(row, col)

    def isHealthy = !infected && !sick && !dead
    def isVisibleInfectious = sick || dead
    def isInfectious = infected || immune

    def canBeInfected = !vaccinated && isHealthy && !immune

    def mobilityReduction = if(isVisibleInfectious) infectedMobilityReductionRate else mobilityReductionRate

    def infect = {
      if(isHealthy && canBeInfected) {
        infected = true
        afterDelay(daysToBecomeSick){ sick = true }
        afterDelay(daysToDie){
          if(probability(deathRate)) {
            dead = true
          } else {
            afterDelay(daysToBecomeImmune - daysToDie){
              sick = false
              immune = true
              afterDelay(daysToTurnHealthy - daysToBecomeImmune){
                infected = false
                immune = false
              }
            }
          }
        }
      }
      infected
    }

    def nextMove: Option[Pos] = {
      if(withAirplane && probability(takeAirplaneRate)) {
        var p = Pos(randomBelow(roomRows), randomBelow(roomColumns))
        while (pos == p)
          p = Pos(randomBelow(roomRows), randomBelow(roomColumns))
        Some(p)
      } else {
        val available = isHealthy match {
          case true => moves.filter { m => !roomHasVisibleInfected(pos.move(m)) }
          case false => moves
        }

        (available match {
          case Nil => None
          case m :: Nil => Some(m)
          case _ => Some(available(randomBelow(available.length)))
        }).map(pos.move(_))
      }

    }

    def move(p: Pos) = {
      if(!dead) {
        row = p.row
        col = p.col
        if(probability(transmissibilityRate) && roomIsInfected(p))
          infect
      }
    }

    def planMove: Unit = {
      if(!dead) {
        afterDelay(randomBelow(maxDaysBeforeMove + 1)){
          if(!reduceMobility || probability(mobilityReduction))
            nextMove.map(move)
          planMove
        }
      }
    }

    planMove
  }

  sealed abstract class Move(var x: Int, var y: Int)

  case object Up extends Move(0, -1)
  case object Down extends Move(0, 1)
  case object Right extends Move(1, 0)
  case object Left extends Move(-1, 0)

  val moves = List(Up, Right, Down, Left)

  case class Pos(row: Int, col: Int) {

    def move(m:Move): Pos = {
      m match {
        case Down | Right => Pos(
          (row + m.x) % roomRows,
          (col + m.y) % roomColumns
        )
        case Up | Left => Pos(
          (row + m.x % roomRows + roomRows) % roomRows,
          (col + m.y % roomColumns + roomColumns) % roomColumns
        )
      }
    }
  }

}

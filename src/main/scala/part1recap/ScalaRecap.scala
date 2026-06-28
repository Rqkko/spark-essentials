package part1recap

object ScalaRecap extends App {

  val aBoolean: Boolean = false

  val anIfExpression = if(2 > 3) "bigger" else "smaller"

  // instructions vs expressions
  val theUnit = println("Hello, Scala") // Unit = no meaningful value (void)

  // functions
  def myFunction(x: Int) =  42

  // OOP
  class Animal
  class Dog extends Animal
  trait Carnivore {
    def eat(animal: Animal): Unit
  }

  class Crocodile extends Animal with Carnivore {
    override def eat(animal: Animal): Unit = println("Crunch!")
  }

  // singleton pattern
  object MySingleton
  // companions
  object Carnivore

  // generics
  trait MyList[A]
}

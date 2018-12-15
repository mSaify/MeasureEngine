package measureenginelab

object B_DataStructures {
  // Tuples have a fixed number of unnamed elements, comma separated, wrapped in parentheses
  val aSimpleTuple: (String, Int) = ("k", 100)

  // A Case Class is terse way to create a compound data structure
  case class Point(x: Int, y: Int)
  val point1 = Point(5, 7)
  val point2 = Point(5, 7)
  println(point1) // Point(5, 7)

  // you get value equality for "free"
  println(point1 == point2) // true

  // case classes are immutable by default. You can make a copy with the copy method:
  val point3 = point2.copy(x = 20)
  println(point3) // Point(20, 7)

  // COLLECTIONS:
  // https://docs.scala-lang.org/overviews/collections/overview.html
  // Perf characteristics: http://docs.scala-lang.org/overviews/collections/performance-characteristics.html

  // Vectors provide efficient indexed access, updating, and appending elements.
  val vector: Vector[Int] = Vector(1, 2, 3)
  val addedItem: Vector[Int] = vector :+ 4
  println(addedItem) // Vector(1, 2, 3, 4)

  // List is a single linked list. Provides efficient access to head and tail.
  val list: List[String] = List("a", "b", "c")
  println(list.tail) // List(b, c)

  // Maps provide access to key value pairs
  val map: Map[String, Int] = Map(("a", 5), ("b", 10))
  println(map("b")) // 10

  // And finally sets
  val set: Set[Int] = Set(6, 7, 8, 6)
  println(set) // Set(6, 7, 8)

  // Typical functional operations can be performed on the collections:
  val multipliedByTwo: Vector[Int] = vector.map(x => x * 2)

  val onlyEven: Vector[Int] = vector.filter(x => x % 2 == 0)

  val summedUp: Int = multipliedByTwo.reduce((x, y) => x + y)
  val summedUp2: Int = multipliedByTwo.fold(0)((acc, y) => acc + y)
  val summedUp3: Int = multipliedByTwo.sum

  // flat map is similar to map except each input can produce zero or more outputs
  val measures = Seq(1, 2, 3)
  val encounters = Seq("a", "b", "c")
  val results: Seq[String] = encounters.flatMap(e => measures.map(m => e + m.toString)) // sort of a cross apply here
  println(results) // List(a1, a2, a3, b1, b2, b3, c1, c2, c3)
}

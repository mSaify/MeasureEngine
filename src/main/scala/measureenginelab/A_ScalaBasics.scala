package measureenginelab

// Stuff lives in a class or an object.
// An object is a singleton, kind of like static members in Java or C#.
object A_ScalaBasics {

  // read only fields are declared with 'val'
  // member name comes first followed by a type
  // here are some primitives
  val readOnlyInt: Int = 5
  val readOnlyLong: Long = 10L
  val readOnlyString: String = "Foo"
  val readOnlyBoolean: Boolean = false

  // types can be inferred
  val anotherInt = 10

  // use 'var' for mutable fields
  var weCanChangeThisLater: Long = 16L

  // declare member functions with 'def'
  def whatIsTheAnswer(): Int = 42

  // parameters go within the parenthesis
  // a function body can contain multiple lines surrounded by braces
  // last expression becomes the return value
  def doSomeMath(a: Double, b: Double): Double = {
    val intermediate = a + b
    intermediate * 2.0
  }

  println(doSomeMath(5.0, 4.0)) // 18.0

  // if statements work as expected. They are an expression producing a value.
  def tankDescription(level: Int): String = {
    if(level <= 0) {
      "Empty"
    } else if(level < 20) {
      "Low"
    } else if(level < 95) {
      "Normal"
    } else {
      "Full"
    }
  }
}

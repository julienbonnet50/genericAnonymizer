object printArgs extends Serializable {

    def main(args: Array[String]): Unit = {
        if (args == null) {
            println("No args provided")
        } else {
            println("List of args provided : ")
            args.zipWithIndex.foreach { case(args, idx) => println(s"ARGS[$idx] ... : [$args]") }
        }
    }
}
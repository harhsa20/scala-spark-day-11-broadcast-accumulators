import org.apache.spark.{SparkConf, SparkContext}

object BroadcastAccumulatorValidation {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Broadcast and Accumulators")
      .setMaster("local[2]")

    val sc = new SparkContext(conf)

    // Small master/reference table
    val productMaster = Map(
      "P101" -> "Laptop",
      "P102" -> "Mouse",
      "P103" -> "Keyboard"
    )

    // Broadcast the small reference data to executors
    val broadcastProducts = sc.broadcast(productMaster)

    // Accumulator to count invalid transactions
    val badRecords = sc.longAccumulator("Bad Records")

    // Transaction data
    val transactions = sc.parallelize(Seq(
      ("T001", "P101", 1),
      ("T002", "P102", 2),
      ("T003", "P999", 1),
      ("T004", "P103", 3),
      ("T005", "P888", 2)
    ))

    // Validate transactions using broadcast data
    val validTransactions = transactions.filter { transaction =>
      val productId = transaction._2

      if (broadcastProducts.value.contains(productId)) {
        true
      } else {
        badRecords.add(1)
        false
      }
    }

    println("Valid Transactions:")
    validTransactions.collect().foreach(println)

    println(s"Bad Records: ${badRecords.value}")

    sc.stop()
  }
}

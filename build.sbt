name := "spark-fim"

version := "1.0"

scalaVersion := "2.11.8"

spName := "hibayesian/spark-fim"

sparkVersion := "2.1.1"

sparkComponents += "mllib"

resolvers += Resolver.sonatypeRepo("public")

spShortDescription := "spark-fim"

spDescription := """A library of scalable frequent itemset mining algorithms based on Spark"""
  .stripMargin

credentials += Credentials(Path.userHome / ".ivy2" / ".sbtcredentials")

licenses += "Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0")
    
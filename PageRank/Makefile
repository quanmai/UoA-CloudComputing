default:
	sbt package
	spark-submit --master local[4] target/scala-2.11/simple-project_2.11-1.0.jar 10

clean:
	rm -rf target project output

clobber:
	rm -rf target project output src/main/scala/*.scala

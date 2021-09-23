default: 
        javac BigramCount.java
        jar -cvf BigramCount.jar ./BigramCount*.class
        hadoop jar BigramCount.jar BigramCount input output

clean:
        rm -rf output *.jar *.class

clobber:
        rm -rf output *.jar *.class *.java

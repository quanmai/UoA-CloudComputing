default: 
        javac WordCount.java
        jar -cvf WordCount.jar ./WordCount*.class
        hadoop jar WordCount.jar WordCount input output

clean:
        rm -rf output *.jar *.class

clobber:
        rm -rf output *.jar *.class *.java

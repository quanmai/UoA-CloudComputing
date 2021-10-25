all:  clean
        javac wordpair.java invertedindex.java
        jar -cvf invertedindex.jar ./*.class
        hadoop jar invertedindex.jar invertedindex input output

clean:
        rm -rf output *.jar *.class

clobber:
        rm -rf output *.jar *.class *.java

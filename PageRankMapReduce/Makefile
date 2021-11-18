default: 
        javac *.java
        jar -cvf PageRank.jar ./*.class
        hadoop jar PageRank.jar PageRank -r 1 -i 1 -n 5 -a 0.1

clean:
        rm -rf output* *.jar *.class

clobber:
        rm -rf output* *.jar *.class *.java
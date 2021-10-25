import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class wordpair implements Writable,WritableComparable<wordpair> {

    private Text word;
    private Text file;

    public wordpair(Text word, Text file) {
        this.word = word;
        this.file = file;
    }

    public wordpair(String word, String file) {
        this(new Text(word),new Text(file));
    }

    public wordpair(String complex) {
        String[] words = complex.split("\\s+");
        this.word = new Text(words[0]);
        this.file = new Text(words[1]);
    }

    public wordpair() {
        this.word = new Text();
        this.file = new Text();
    }

    @Override
    public int compareTo(wordpair other) {                         // A compareTo B
        /*int returnVal = this.word.compareTo(other.getWord());      // return -1: A < B
        if(returnVal != 0){                                        // return 0: A = B
            return returnVal;}                                      // return 1: A > B
        else return this.file.compareTo(other.getFile());*/
        return this.word.compareTo(other.getWord());
        /*if(this.file.toString().equals("*")){
            return -1;
        }else if(other.getFile().toString().equals("*")){
            return 1;
        }
        return this.file.compareTo(other.getFile());*/
    }

    public static wordpair read(DataInput in) throws IOException {
        wordpair wordPair = new wordpair();
        wordPair.readFields(in);
        return wordPair;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        word.write(out);
        file.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        word.readFields(in);
        file.readFields(in);
    }

    @Override
    public String toString() {
        return this.word.toString() + " " + this.file.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        wordpair wordPair = (wordpair) o;

        if (file != null ? !file.equals(wordPair.file) : wordPair.file != null) return false;
        if (word != null ? !word.equals(wordPair.word) : wordPair.word != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (word != null) ? word.hashCode() : 0;
        result = 163 * result;// + ((file != null) ? file.hashCode() : 0);
        //System.out.println(result);
        return result;
    }

    public void setWord(String word){
        this.word.set(word);
    }
    public void setFile(String file){
        this.file.set(file);
    }

    public Text getWord() {
        return word;
    }

    public Text getFile() {
        return file;

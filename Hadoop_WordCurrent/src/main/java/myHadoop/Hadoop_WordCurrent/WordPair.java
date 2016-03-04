package myHadoop.Hadoop_WordCurrent;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public  class WordPair implements WritableComparable<WordPair>{
	private String word1;
	private String word2;
	public WordPair(){		
	}
	
	public WordPair(String wordA,String wordB){
		this.word1 = wordA;
		this.word2 = wordB;
	}
	
	public String getWord1(){
		return this.word1;
	}
	
	public String getWord2(){
		return this.word2;
	}
	
	public int hashCode(){
		return (word1.hashCode() + word2.hashCode()) * 17;
	}
	public void write(DataOutput out) throws IOException {
		out.writeUTF(word1);
		out.writeUTF(word2);
	}

	public void readFields(DataInput in) throws IOException {
		word1 = in.readUTF();
		word2 = in.readUTF();
	}

	public int compareTo(WordPair o) {
		if(this.equals(o))
			return 0;
		else
			return (word1 + word2).compareTo(o.getWord1() + o.getWord2());
	}
	
	public String toString(){
		return word1 + "," + word2;
	}
	
	public boolean equals(Object o){
		//无序对，不用考虑顺序
		if(!(o instanceof WordPair))
			return false;
		WordPair w = (WordPair)o;
		if((this.word1.equals(w.word1) && this.word2.equals(w.word2))
				|| (this.word2.equals(w.word1) && this.word1.equals(w.word2)))
				return true;
		return false;
	}

}

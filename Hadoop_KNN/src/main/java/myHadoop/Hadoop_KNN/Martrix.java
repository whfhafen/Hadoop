package myHadoop.Hadoop_KNN;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;

public class Martrix {
	//存放数据转化而成的数组
	ArrayList<Integer> Ma;
	private int Lable;
	
	public Martrix(){
		Lable = -1;
		Ma = null;
	}
	public Martrix(FSDataInputStream fs) throws NumberFormatException, IOException{
		String line;
		//Lable在外面设置
		Ma = new ArrayList<Integer>();
		Character ch;
		BufferedReader br = new BufferedReader(new InputStreamReader(fs,"UTF-8"));
			while ((line=br.readLine())!=null){
				for(int i=0;i<line.length();i++){
					ch = line.charAt(i);
					Ma.add(Integer.parseInt(ch.toString()));
				}
				
	}
			br.close();
	}
	public ArrayList<Integer> getMa() {
		return Ma;
	}

	public void setMa(ArrayList<Integer> ma) {
		Ma = ma;
	}

	public int getLable() {
		return Lable;
	}

	public void setLable(int lable) {
		Lable = lable;
	}
	
}

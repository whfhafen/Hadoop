package myHadoop.Hadoop_KNN;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.Waitable;

public class ListWritable<T extends Writable> implements Writable{
	private List<T> list;
	private Class<T> clazz;
	public ListWritable(){
		this.setList(null);
		this.setClazz(null);
	}
	public ListWritable(Class<T> clazz){
		this.setClazz(clazz);
		setList(new ArrayList<T>());
	}
	
	public void add(T element){
		list.add(element);
	}
	
	public boolean isEmpty(){
		return list.isEmpty();
	}
	
	public int size(){
		return list.size();
	} 
	public void add(int index,T element){
		list.add(index, element);
	}
	
	public T get(int index){
		return list.get(index);
	}
	
	public T remove(int index){
		return list.remove(index);
	}
	
	public void set(int index,T element){
		list.set(index, element);
	}
	
	public void write(DataOutput out) throws IOException {
	    out.writeUTF(clazz.getName());
	    out.writeInt(list.size());
	    for (T element : list) {
	        element.write(out);
	    }
	 }

	 @SuppressWarnings("unchecked")
	public void readFields(DataInput in) throws IOException{
	 try {
		clazz = (Class<T>) Class.forName(in.readUTF());
	} catch (ClassNotFoundException e1) {
		// TODO Auto-generated catch block
		e1.printStackTrace();
	}
	 int count = in.readInt();
	 this.list = new ArrayList<T>();
	 for (int i = 0; i < count; i++) {
	    try {
	        T obj = clazz.newInstance();
	        obj.readFields(in);
	        list.add(obj);
	    } catch (InstantiationException e) {
	        e.printStackTrace();
	    } catch (IllegalAccessException e) {
	        e.printStackTrace();
	    }
	  }
	}
	public List<T> getList() {
		return list;
	}
	public void setList(ArrayList<T> trainLable) {
		this.list = trainLable;
	}
	public Class<T> getClazz() {
		return clazz;
	}
	public void setClazz(Class<T> clazz) {
		this.clazz = clazz;
	}

}

package myHadoop.Hadoop_kmeans;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Writable;
  //自定义数据类型存放簇中心点信息
public class Instance implements Writable{
	ArrayList<Double> value;
	public Instance (){
		value = new ArrayList<Double>();
	}
	/**以行为单位读入数据*/
	public Instance (String line){
		String[] values = line.split(",");
		value = new ArrayList<Double>();
		for(String va:values){
			value.add(Double.parseDouble(va));
		}
	}
	/**将一个Instance的数据添加到value中*/
	public Instance (Instance ins){
		value = new ArrayList<Double>();
		for(int i = 0;i<ins.getvalue().size();i++){
			value.add(ins.getvalue().get(i));
		}
	}
	/**ArrayList getvalue方法*/
	public ArrayList<Double> getvalue(){
		return value;
		
	}
	
	public Instance (int k){
		value = new ArrayList<Double>();
		for(int i = 0; i < k; i++){
			value.add(0.0);
		}
	}
	
	/**重写加运算*/
	public Instance add(Instance instance){
		if(value.size()==0)
			return new Instance(instance);
		else if(instance.getvalue().size()==0)
			return new Instance(this);
		else if (value.size()!=instance.getvalue().size())
			try {
				
				throw new Exception("can not add! dimension not compatible!" + value.size() + ","
						+ instance.getvalue().size());
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return null;
			}
		else{
			Instance result = new Instance();
			for(int i=0;i<value.size();i++){
			result.getvalue().add(value.get(i)+instance.getvalue().get(i));
			}
			return result;
		}	
	}
	
	/**重写乘运算*/
	public Instance multiply(double num){
		Instance result = new Instance();
		for(int i = 0; i < value.size(); i++){
			result.getvalue().add(value.get(i) * num);
		}
		return result;
		
	}
	public Instance divide(double num){
		Instance result = new Instance();
		for(int i = 0; i < value.size(); i++){
			result.getvalue().add(value.get(i) / num);
		}
		return result;
	}
	
	public String toString(){
		String s = new String();
		for(int i = 0;i < value.size() - 1; i++){
			s += (value.get(i) + ",");
		}
		s += value.get(value.size() - 1);
		return s;
	}
	
	public void write(DataOutput out) throws IOException {
		out.writeInt(value.size());
		for(int i = 0; i < value.size(); i++){
			out.writeDouble(value.get(i));
		}
	}

	public void readFields(DataInput in) throws IOException {
		int size = 0;
		value = new ArrayList<Double>();
		if((size = in.readInt()) != 0){
			for(int i = 0; i < size; i++){
				value.add(in.readDouble());
			}
		}
	}
	}

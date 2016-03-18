package myHadoop.Hadoop_KNN;

public class instance {
	private double[] arrtibuteVule;
	private double lable;
	public instance(String line){
		String[] value = line.split(" ");
		for(int i=0;i<value.length-1;i++){
			this.arrtibuteVule[i]=Double.parseDouble(value[i]);
		}
		this.setLable(Double.parseDouble(value[value.length-1]));
	}
	public double[] getarrtibuteVule(){
		return arrtibuteVule;
		
	}
	public double getLable() {
		return lable;
	}
	public void setLable(double lable) {
		this.lable = lable;
	}
}

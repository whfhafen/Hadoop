package myHadoop.Hadoop_computer;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

public class KPI {
	private String request_addr;// 调用方ip地址
	private String request_time;// 调用的时间
	private String request_Http;// HTTP请求
	private String request_URL; // URL
	private String request_GET; // HTTP METROD
	private String request_stat; //HTTP请求的状态
	private String return_length;//返回的字节长度
	private String echo_Time; //请求的返回时间
	
	
	private boolean valid = true; //判断数据是否合法
	
	public static KPI split_sr(String line){
		KPI kpi = new KPI();
		String[] sr = line.split(" ");
		if(sr.length==10){
			kpi.setRequest_addr(sr[0]);
			try {
				kpi.setRequest_time(sr[1].substring(1));
			} catch (ParseException e) {
				e.printStackTrace();
			}
			kpi.setRequest_stat(sr[7]);
		}else{
			kpi.setValid(false);
		}
		return kpi;
		
	}
//	public static void main(String[] args) throws ParseException {
//		String s ="172.22.49.45 [08/Sep/2015:00:25:37 +0800] \"GET /tour/product/query HTTP/1.1\" GET 200 100 1";
//		String[] er = s.split(" ");
//		for(String d :er){
//			System.out.println(d);
//		}
//		KPI kpi = new KPI();
//		kpi.setRequest_time(er[1].substring(1));
//		System.out.println(kpi.getRequest_time());
//	}
	public String getRequest_addr() {
		return request_addr;
	}
	public void setRequest_addr(String request_addr) {
		this.request_addr = request_addr;
	}
	public String getRequest_time() {
		return request_time;
	}
	public void setRequest_time(String request_time) throws ParseException {
		SimpleDateFormat date = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.US);
		Date df1 = date.parse(request_time);
		SimpleDateFormat df2 = new SimpleDateFormat("HH");
		this.request_time = df2.format(df1);
	}
	public String getRequest_Http() {
		return request_Http;
	}
	public void setRequest_Http(String request_Http) {
		this.request_Http = request_Http;
	}
	public String getRequest_URL() {
		return request_URL;
	}
	public void setRequest_URL(String request_URL) {
		this.request_URL = request_URL;
	}
	public String getRequest_GET() {
		return request_GET;
	}
	public void setRequest_GET(String request_GET) {
		this.request_GET = request_GET;
	}
	public String getRequest_stat() {
		return request_stat;
	}
	public void setRequest_stat(String request_stat) {
		this.request_stat = request_stat;
	}
	public String getReturn_length() {
		return return_length;
	}
	public void setReturn_length(String return_length) {
		this.return_length = return_length;
	}
	public String getEcho_Time() {
		return echo_Time;
	}
	public void setEcho_Time(String echo_Time) {
		this.echo_Time = echo_Time;
	}
	public boolean isValid() {
		return valid;
	}
	public void setValid(boolean valid) {
		this.valid = valid;
	}
}

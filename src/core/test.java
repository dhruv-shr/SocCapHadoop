package core;

public class test {

	public static void main(String args[]){
		
		String a = "pranjul" + CommonConstants.TAB + "richie" + CommonConstants.TAB + "jam" + CommonConstants.TAB + "ant";
		
		String key = a.substring(0, a.indexOf(CommonConstants.TAB));
		String value = a.substring(a.indexOf(CommonConstants.TAB)+1);
		System.out.println(key);
		System.out.println(value);
	}
}

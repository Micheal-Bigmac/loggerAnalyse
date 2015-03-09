package shuffeData;

public class parserLogger {
	public static final String  PatterRegex="(.*) - - \\[(.*) \\+.*] \"(.*)\" (.*) (.*)";
	public static String[] parse(String line){
		return line.replaceAll(PatterRegex, "$1--$2--$3--$4--$5").split("--");
	}
	public static void main(String[] args) {
		String testData="27.19.74.143 - - [30/May/2013:17:38:20 +0800] \"GET /static/image/common/faq.gif HTTP/1.1\" 200 1127";
		System.out.println(testData);
		String []values=parserLogger.parse(testData);
		for(String tmp : values){
			System.out.println(tmp);
		}
	}
}
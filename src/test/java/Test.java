import java.util.Arrays;

public class Test {
	
	public static void main(String[] args) {
		
		Arrays.asList("a,b c,c d, d , e, ".split(" *, *")).forEach(s -> System.out.println("---" + s + "---"));
	}
}
